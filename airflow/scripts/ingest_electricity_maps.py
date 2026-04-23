"""
Ingesta Bronze desde Electricity Maps → S3 (o disco) bajo electricity_maps/.

Cubre el contrato de `docs/DICCIONARIO_DE_DATOS.md` §2:
  §2.1 Carbon intensity — latest, past (un datetime), history (ventana ~24 h API v4)
  §2.2 Electricity mix — latest (única temporalidad en el diccionario)

Modos:
  --mode latest   → pensado para Airflow frecuente (snapshot actual + past 1h + history 24h)
  --mode history  → backfill de carbono con /v4/carbon-intensity/past-range (ventanas de 10 días)

Variables: .env (python-dotenv), ELECTRICITY_MAPS_TOKEN; S3: AWS_S3_BUCKET, etc.
  ELECTRICITY_MAPS_USER_AGENT — opcional; Cloudflare puede devolver 1010 si se usa el UA por defecto de urllib.

Zonas: data/geo_cloud_to_country_and_zones.csv o --zone.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None  # type: ignore[misc, assignment]
    ClientError = BotoCoreError = Exception  # type: ignore[misc, assignment]

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]


DEFAULT_API_BASE = "https://api.electricitymaps.com"
REFERENCE_ZONES_CSV = "data/geo_cloud_to_country_and_zones.csv"
# Cloudflare 1010 bloquea con frecuencia el User-Agent "Python-urllib/…"; alineado a curl que suele funcionar.
_DEFAULT_USER_AGENT = "curl/8.7.1"
# Límite documentado: 10 días (240 h) por petición en granularidad horaria.
_PAST_RANGE_MAX_CHUNK = timedelta(days=10)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _utc_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _parse_iso_datetime(s: str) -> datetime:
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _datetime_slug(dt: datetime) -> str:
    return _iso_z(dt).replace(":", "-")


def _load_zones_from_csv(path: Path) -> list[str]:
    if not path.is_file():
        print(f"[error] no existe el CSV de zonas: {path}", file=sys.stderr)
        return []
    out: set[str] = set()
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "electricity_maps_zone" not in (reader.fieldnames or []):
            print("[error] falta columna electricity_maps_zone en el CSV.", file=sys.stderr)
            return []
        for row in reader:
            z = (row.get("electricity_maps_zone") or "").strip()
            if z:
                out.add(z)
    return sorted(out)


def _query_string(params: dict[str, str | bool | None]) -> str:
    pairs: list[tuple[str, str]] = []
    for k, v in params.items():
        if v is None or v == "":
            continue
        if isinstance(v, bool):
            pairs.append((k, "true" if v else "false"))
        else:
            pairs.append((k, str(v)))
    return urllib.parse.urlencode(pairs)


def _fetch(
    url: str,
    token: str,
    timeout_s: int,
    user_agent: str,
) -> tuple[int, bytes]:
    req = urllib.request.Request(
        url,
        headers={
            "auth-token": token,
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return resp.getcode() or 200, resp.read()
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        return e.code, body
    except urllib.error.URLError as e:
        print(f"[error] red {url}: {e}", file=sys.stderr)
        return 0, b""


def _put_s3(
    bucket: str,
    key: str,
    body: bytes,
    profile: str | None,
    region: str | None,
) -> None:
    if boto3 is None:
        raise RuntimeError("Instala boto3: pip install boto3")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name=region)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def _write_local(root: Path, key_suffix: str, body: bytes) -> Path:
    path = root / "bronze" / "electricity_maps" / key_suffix.replace("/", os.sep)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def _common_api_params(args: argparse.Namespace) -> dict[str, str | bool | None]:
    d: dict[str, str | bool | None] = {}
    if getattr(args, "temporal_granularity", None):
        d["temporalGranularity"] = args.temporal_granularity
    if getattr(args, "emission_factor_type", None):
        d["emissionFactorType"] = args.emission_factor_type
    if getattr(args, "disable_estimations", False):
        d["disableEstimations"] = True
    return d


def _default_past_datetime_utc() -> str:
    now = datetime.now(timezone.utc)
    hour_floor = now.replace(minute=0, second=0, microsecond=0)
    prev = hour_floor - timedelta(hours=1)
    return _iso_z(prev)


def _iter_past_range_chunks(
    start: datetime,
    end: datetime,
    chunk: timedelta,
) -> list[tuple[datetime, datetime]]:
    if start >= end:
        return []
    out: list[tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        nxt = min(cur + chunk, end)
        out.append((cur, nxt))
        cur = nxt
    return out


def main() -> int:
    root = _repo_root()
    if load_dotenv is not None:
        load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(
        description="Ingesta Bronze: Electricity Maps (diccionario §2) → S3 electricity_maps/.",
    )
    parser.add_argument(
        "--mode",
        choices=("latest", "history"),
        default="latest",
        help="latest: señales en tiempo casi real + past + history 24h. history: carbon past-range en ventanas.",
    )
    parser.add_argument(
        "--api-base",
        default=os.environ.get("ELECTRICITY_MAPS_API_BASE", DEFAULT_API_BASE),
        help="Origen de la API.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("AWS_S3_BUCKET"),
        help="Bucket S3.",
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE"),
        help="Perfil AWS CLI.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION"),
        help="Región del cliente S3.",
    )
    parser.add_argument(
        "--zones-csv",
        default=str(root / REFERENCE_ZONES_CSV),
        help="CSV con electricity_maps_zone.",
    )
    parser.add_argument(
        "--zone",
        action="append",
        dest="zones_override",
        metavar="ZONE",
        help="Zona EM (repetible).",
    )
    parser.add_argument(
        "--history-start",
        metavar="ISO",
        help="Modo history: inicio del rango (ISO 8601, UTC).",
    )
    parser.add_argument(
        "--history-end",
        metavar="ISO",
        help="Modo history: fin del rango (exclusivo en API; ISO 8601, UTC).",
    )
    parser.add_argument(
        "--past-datetime",
        metavar="ISO",
        help="Modo latest: datetime para carbon/mix past (default: hora UTC anterior completa).",
    )
    parser.add_argument(
        "--temporal-granularity",
        default=None,
        help="p. ej. hourly, 15_minutes (según plan API).",
    )
    parser.add_argument(
        "--emission-factor-type",
        choices=("lifecycle", "direct"),
        default=None,
        help="Solo endpoints de carbono.",
    )
    parser.add_argument(
        "--disable-estimations",
        action="store_true",
        help="disableEstimations=true en la query.",
    )
    parser.add_argument(
        "--mix-breakdown-type",
        choices=("normal", "flow-traced"),
        default=None,
        help="Solo electricity-mix/*.",
    )
    parser.add_argument(
        "--skip-mix",
        action="store_true",
        help="No llamar electricity-mix/*.",
    )
    parser.add_argument(
        "--skip-carbon",
        action="store_true",
        help="No llamar carbon-intensity/* (latest, past, history o past-range).",
    )
    parser.add_argument(
        "--skip-carbon-past",
        action="store_true",
        help="Modo latest: no llamar carbon-intensity/past.",
    )
    parser.add_argument(
        "--skip-carbon-history",
        action="store_true",
        help="Modo latest: no llamar carbon-intensity/history (24 h).",
    )
    parser.add_argument(
        "--skip-zone-endpoints",
        action="store_true",
        help="No iterar zonas (solo catálogo si no lo omites).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )
    parser.add_argument(
        "--local-bronze",
        action="store_true",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
    )
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("ELECTRICITY_MAPS_USER_AGENT") or _DEFAULT_USER_AGENT,
        help="Cabecera User-Agent (evita Cloudflare 1010 con urllib). Env: ELECTRICITY_MAPS_USER_AGENT.",
    )
    args = parser.parse_args()

    token = (os.environ.get("ELECTRICITY_MAPS_TOKEN") or "").strip()
    if not args.dry_run and not token:
        print("[error] define ELECTRICITY_MAPS_TOKEN.", file=sys.stderr)
        return 1

    if args.zones_override:
        zones = sorted({z.strip() for z in args.zones_override if z.strip()})
    else:
        zones = _load_zones_from_csv(Path(args.zones_csv))

    fetch_per_zone = not args.skip_zone_endpoints
    if args.mode == "history":
        if not args.history_start or not args.history_end:
            print("[error] modo history requiere --history-start y --history-end.", file=sys.stderr)
            return 1
        h_start = _parse_iso_datetime(args.history_start)
        h_end = _parse_iso_datetime(args.history_end)
        if h_start >= h_end:
            print("[error] history-start debe ser < history-end.", file=sys.stderr)
            return 1

    if fetch_per_zone and not zones:
        print("[error] no hay zonas. Usa --zone o rellena electricity_maps_zone en el CSV.", file=sys.stderr)
        return 1

    bucket = args.bucket
    if not args.dry_run and not args.local_bronze:
        if not bucket:
            print("[error] falta --bucket o AWS_S3_BUCKET.", file=sys.stderr)
            return 1
        if boto3 is None:
            print("[error] instala boto3.", file=sys.stderr)
            return 1

    base = args.api_base.rstrip("/")
    run_slug = _utc_slug()
    errors = 0
    common = _common_api_params(args)

    def persist(key_suffix: str, body: bytes) -> None:
        nonlocal errors
        if args.dry_run:
            print(f"[dry-run] -> s3://{bucket}/{key_suffix}" if bucket else f"[dry-run] -> {key_suffix}")
            return
        if args.local_bronze:
            p = _write_local(root, key_suffix, body)
            print(f"OK local {p}")
            return
        try:
            _put_s3(bucket, key_suffix, body, args.profile, args.region)
            print(f"OK s3://{bucket}/{key_suffix}")
        except (ClientError, BotoCoreError, OSError, RuntimeError) as e:
            print(f"[error] S3 {key_suffix}: {e}", file=sys.stderr)
            errors += 1

    def get_and_persist(url: str, key_suffix: str) -> bool:
        nonlocal errors
        if args.dry_run:
            print(f"[dry-run] GET {url}")
            persist(key_suffix, b"{}")
            return True
        code, body = _fetch(url, token, args.timeout, args.user_agent)
        if code == 200 and body:
            persist(key_suffix, body)
            return True
        print(f"[error] HTTP {code} {url} body[:200]={body[:200]!r}", file=sys.stderr)
        errors += 1
        return False

    if args.mode == "history" and fetch_per_zone and not args.skip_carbon:
        chunks = _iter_past_range_chunks(h_start, h_end, _PAST_RANGE_MAX_CHUNK)
        for zone in zones:
            for c_start, c_end in chunks:
                if args.sleep_ms > 0:
                    time.sleep(args.sleep_ms / 1000.0)
                params = {
                    "zone": zone,
                    "start": _iso_z(c_start),
                    "end": _iso_z(c_end),
                    **common,
                }
                qs = _query_string(params)
                url = f"{base}/v4/carbon-intensity/past-range?{qs}"
                start_slug = _datetime_slug(c_start)
                end_slug = _datetime_slug(c_end)
                key = (
                    f"electricity_maps/carbon_intensity/past-range/"
                    f"zone={zone}/start={start_slug}/end={end_slug}/{run_slug}.json"
                )
                get_and_persist(url, key)

    if args.mode == "latest" and fetch_per_zone:
        past_dt = args.past_datetime or _default_past_datetime_utc()
        mix_params: dict[str, str | bool | None] = {}
        if args.mix_breakdown_type:
            mix_params["breakdownType"] = args.mix_breakdown_type
        mix_params.update(common)

        for zone in zones:
            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

            if not args.skip_mix:
                mp = {**mix_params, "zone": zone}
                qs = _query_string(mp)
                url = f"{base}/v4/electricity-mix/latest?{qs}"
                key = f"electricity_maps/electricity_mix/latest/zone={zone}/{run_slug}.json"
                get_and_persist(url, key)

            if args.skip_carbon:
                continue

            cp = {**common, "zone": zone}
            qs = _query_string(cp)
            url = f"{base}/v4/carbon-intensity/latest?{qs}"
            key = f"electricity_maps/carbon_intensity/latest/zone={zone}/{run_slug}.json"
            get_and_persist(url, key)

            if not args.skip_carbon_past:
                cpp = {**cp, "datetime": past_dt}
                qs = _query_string(cpp)
                url = f"{base}/v4/carbon-intensity/past?{qs}"
                past_slug = past_dt.replace(":", "-").replace(".", "-")
                key = f"electricity_maps/carbon_intensity/past/zone={zone}/datetime={past_slug}/{run_slug}.json"
                get_and_persist(url, key)

            if not args.skip_carbon_history:
                qs = _query_string(cp)
                url = f"{base}/v4/carbon-intensity/history?{qs}"
                key = f"electricity_maps/carbon_intensity/history/zone={zone}/{run_slug}.json"
                get_and_persist(url, key)

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
