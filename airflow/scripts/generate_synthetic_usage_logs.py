"""
Genera logs sintéticos de sesiones de uso de IA alineados con docs/DICCIONARIO_DE_DATOS.md §4.

Integridad referencial: regiones solo desde impact.csv (AWS); GPUs e instancias desde los CSV MLCO2.
Movilidad: cada user_id tiene un conjunto de regiones muestreadas del catálogo para sesiones en distintas zonas.
Casos borde: ~1% de filas con duration_hours inválido (vacío o negativo) para pruebas en Silver.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

# Orden de columnas = secciones 4.1 y 4.2 del diccionario
OUTPUT_FIELDS = [
    "session_id",
    "user_id",
    "timestamp",
    "gpu_model",
    "region",
    "duration_hours",
    "instance_type",
    "gpu_utilization",
    "job_type",
    "energy_consumed_kwh",
    "execution_status",
]

JOB_TYPES = ("Training", "Inference", "Fine-tuning")
NUM_USERS = 500
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Solo con --allow-fallback (desarrollo sin CSV); no usar en pipelines que hacen join con MLCO2.
FALLBACK_INSTANCES: list[tuple[str, str]] = [
    ("p3.2xlarge", "Tesla V100"),
    ("p3.8xlarge", "Tesla V100"),
    ("p2.xlarge", "Tesla K80"),
    ("g4dn.xlarge", "T4"),
]
FALLBACK_GPU_TDP: dict[str, float] = {
    "Tesla V100": 300.0,
    "Tesla K80": 300.0,
    "T4": 70.0,
}
FALLBACK_AWS_REGIONS = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "sa-east-1",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _parse_tdp(raw: str) -> float | None:
    s = (raw or "").strip().lower()
    if not s or s == "nan":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_gpus(mlco2_dir: Path) -> list[dict[str, str]]:
    rows = _read_csv_rows(mlco2_dir / "gpus.csv")
    out: list[dict[str, str]] = []
    for r in rows:
        if (r.get("type") or "").strip().lower() != "gpu":
            continue
        if _parse_tdp(r.get("tdp_watts", "") or "") is None:
            continue
        out.append(r)
    return out


def load_instances(mlco2_dir: Path) -> list[tuple[str, str]]:
    rows = _read_csv_rows(mlco2_dir / "instances.csv")
    pairs: list[tuple[str, str]] = []
    for r in rows:
        prov = (r.get("provider") or "").strip().lower()
        if prov != "aws":
            continue
        iid = (r.get("id") or "").strip()
        gpu = (r.get("gpu") or "").strip()
        if iid and gpu:
            pairs.append((iid, gpu))
    return pairs


def load_aws_regions(mlco2_dir: Path) -> list[str]:
    """Códigos `region` del catálogo MLCO2 impact.csv (solo filas provider=aws)."""
    rows = _read_csv_rows(mlco2_dir / "impact.csv")
    regions: list[str] = []
    for r in rows:
        if (r.get("provider") or "").strip().lower() != "aws":
            continue
        reg = (r.get("region") or "").strip()
        if reg and reg not in regions:
            regions.append(reg)
    return regions


def build_user_region_pools(
    rng: random.Random,
    regions: list[str],
    num_users: int,
    pool_size: int = 4,
) -> list[list[str]]:
    """
    Por usuario, un subconjunto aleatorio de regiones del catálogo (siempre del mismo impact.csv)
    para que el mismo user_id aparezca en varias regiones (movilidad de cómputo).
    """
    if not regions:
        return [[] for _ in range(num_users)]
    k = min(max(2, pool_size), len(regions))
    pools: list[list[str]] = []
    for _ in range(num_users):
        pools.append(rng.sample(regions, k=k))
    return pools


def validate_instances_against_gpus(instances: list[tuple[str, str]], gpus: list[dict[str, str]]) -> None:
    """Falla si algún tipo de instancia no mapea a una fila de gpus.csv (evita sorpresas en el join)."""
    probe = random.Random(0)
    for iid, short in instances:
        try:
            match_gpu_catalog(short, gpus, probe)
        except ValueError as e:
            raise ValueError(f"instances.csv ({iid!r} → gpu {short!r}): {e}") from e


def match_gpu_catalog(short_name: str, gpus: list[dict[str, str]], rng: random.Random) -> tuple[str, float]:
    """Devuelve (nombre canónico MLCO2, TDP en vatios)."""
    short = short_name.strip()
    exact = [g for g in gpus if (g.get("name") or "").strip() == short]
    if exact:
        g = rng.choice(exact)
        tdp = _parse_tdp(g.get("tdp_watts", "") or "")
        assert tdp is not None
        return g["name"].strip(), tdp

    prefix = [g for g in gpus if (g.get("name") or "").strip().startswith(short)]
    if prefix:
        g = rng.choice(prefix)
        tdp = _parse_tdp(g.get("tdp_watts", "") or "")
        assert tdp is not None
        return g["name"].strip(), tdp

    if short in FALLBACK_GPU_TDP:
        return short, FALLBACK_GPU_TDP[short]

    raise ValueError(f"No hay TDP/catálogo para gpu '{short_name}'")


def duration_hours_normal(rng: random.Random) -> float:
    """Distribución normal acotada a [0.1, 24.0] (diccionario §4.1, filas válidas)."""
    for _ in range(50):
        x = rng.gauss(12.0, 4.0)
        if 0.1 <= x <= 24.0:
            return round(x, 4)
    return round(rng.uniform(0.1, 24.0), 4)


def build_row(
    fake: Faker,
    rng: random.Random,
    instances: list[tuple[str, str]],
    gpus: list[dict[str, str]],
    user_region_pools: list[list[str]],
    edge_invalid_duration: bool,
) -> dict[str, Any]:
    instance_type, gpu_short = rng.choice(instances)
    gpu_model, tdp_w = match_gpu_catalog(gpu_short, gpus, rng)

    user_n = rng.randint(0, NUM_USERS - 1)
    user_id = f"USER_{user_n:03d}"
    pool = user_region_pools[user_n]
    region = rng.choice(pool)

    ts = fake.date_time_between(start_date=START_DATE, end_date="now", tzinfo=timezone.utc)
    timestamp_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    gpu_utilization = round(rng.uniform(0.1, 1.0), 4)
    job_type = rng.choice(JOB_TYPES)

    if edge_invalid_duration:
        if rng.random() < 0.5:
            duration_hours_val: str | float = ""
            energy_kwh_val: str | float = ""
        else:
            duration_hours_val = round(rng.uniform(-48.0, -0.01), 4)
            energy_kwh_val = ""
    else:
        d = duration_hours_normal(rng)
        duration_hours_val = d
        energy_kwh_val = round((d * tdp_w * gpu_utilization) / 1000.0, 6)

    execution_status = "Success" if rng.random() < 0.95 else "Failed"

    return {
        "session_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": timestamp_str,
        "gpu_model": gpu_model,
        "region": region,
        "duration_hours": duration_hours_val,
        "instance_type": instance_type,
        "gpu_utilization": gpu_utilization,
        "job_type": job_type,
        "energy_consumed_kwh": energy_kwh_val,
        "execution_status": execution_status,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera CSV de logs sintéticos (diccionario §4).")
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Número de registros (recomendado 10k–100k).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Semilla para reproducibilidad.")
    parser.add_argument(
        "--mlco2-dir",
        type=Path,
        default=None,
        help="Carpeta con gpus.csv, instances.csv, impact.csv (por defecto data/Code_Carbon bajo la raíz del repo).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta del CSV de salida (por defecto data/usage_logs.csv).",
    )
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Si faltan CSV MLCO2, usar catálogos mínimos incrustados (rompe integridad con impact real).",
    )
    parser.add_argument(
        "--edge-case-rate",
        type=float,
        default=0.01,
        help="Fracción de filas con duration_hours inválido (vacío o negativo); 0 desactiva.",
    )
    parser.add_argument(
        "--mobility-regions-per-user",
        type=int,
        default=4,
        help="Cuántas regiones distintas (todas de impact.csv) asignar a cada usuario para movilidad.",
    )
    args = parser.parse_args()

    if args.rows < 1:
        print("--rows debe ser >= 1", file=sys.stderr)
        return 1
    if not 0.0 <= args.edge_case_rate <= 1.0:
        print("--edge-case-rate debe estar entre 0 y 1", file=sys.stderr)
        return 1

    root = _repo_root()
    mlco2_dir = args.mlco2_dir or (root / "data" / "Code_Carbon")

    gpus = load_gpus(mlco2_dir)
    instances = load_instances(mlco2_dir)
    regions = load_aws_regions(mlco2_dir)

    if not args.allow_fallback:
        missing: list[str] = []
        if not regions:
            missing.append(f"sin regiones AWS en impact.csv ({mlco2_dir})")
        if not instances:
            missing.append(f"sin instancias aws en instances.csv ({mlco2_dir})")
        if not gpus:
            missing.append(f"sin GPUs válidas en gpus.csv ({mlco2_dir})")
        if missing:
            for m in missing:
                print(f"Error: {m}. Copia los CSV MLCO2 o usa --allow-fallback.", file=sys.stderr)
            return 1
    else:
        if not instances:
            instances = list(FALLBACK_INSTANCES)
        if not regions:
            regions = list(FALLBACK_AWS_REGIONS)
        if not gpus:
            print(
                "Aviso: gpus.csv ausente o vacío; TDP solo desde catálogo de respaldo.",
                file=sys.stderr,
            )

    if gpus and instances:
        try:
            validate_instances_against_gpus(instances, gpus)
        except ValueError as e:
            print(f"Error de integridad catálogo: {e}", file=sys.stderr)
            return 1

    rng = random.Random(args.seed)
    Faker.seed(args.seed)
    fake = Faker()
    fake.seed_instance(args.seed)

    user_region_pools = build_user_region_pools(
        rng, regions, NUM_USERS, pool_size=args.mobility_regions_per_user
    )

    out_path = args.output or (root / "data" / "usage_logs.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    edge_count = 0
    target_edges = int(round(args.rows * args.edge_case_rate)) if args.edge_case_rate > 0 else 0
    edge_indices: set[int] = set()
    if target_edges > 0:
        edge_indices = set(rng.sample(range(args.rows), k=min(target_edges, args.rows)))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for i in range(args.rows):
            is_edge = i in edge_indices
            if is_edge:
                edge_count += 1
            row = build_row(
                fake,
                rng,
                instances,
                gpus,
                user_region_pools,
                edge_invalid_duration=is_edge,
            )
            w.writerow(row)

    print(
        f"Escritos {args.rows} registros en {out_path} "
        f"({edge_count} filas con duration/energia no validas para pruebas Silver)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
