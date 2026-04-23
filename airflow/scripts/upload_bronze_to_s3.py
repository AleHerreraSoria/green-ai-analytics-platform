"""
Sube a S3 los CSV locales alineados a los prefijos del bucket (usage_logs, mlco2, owid, etc.).

Variables: se lee `.env` en la raíz del repo (python-dotenv) si está instalado.
Bucket: AWS_S3_BUCKET; región: AWS_DEFAULT_REGION; perfil: AWS_PROFILE o --profile.
Credenciales: perfil ~/.aws/credentials, variables AWS_ACCESS_KEY_* o IAM role.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Instala boto3: pip install boto3", file=sys.stderr)
    raise SystemExit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


# (ruta relativa a la raíz del repo, clave S3 sin nombre de bucket)
DEFAULT_UPLOADS: list[tuple[str, str]] = [
    (
        "bronze/reference/geo_cloud_to_country_and_zones.csv",
        "reference/geo_cloud_to_country_and_zones.csv",
    ),
    (
        "bronze/reference/aws_ec2_on_demand_usd_per_hour.csv",
        "reference/aws_ec2_on_demand_usd_per_hour.csv",
    ),
    ("bronze/usage_logs/usage_logs.csv", "usage_logs/usage_logs.csv"),
    ("data/Code_Carbon/gpus.csv", "mlco2/gpus.csv"),
    ("data/Code_Carbon/instances.csv", "mlco2/instances.csv"),
    ("data/Code_Carbon/impact.csv", "mlco2/impact.csv"),
    ("data/Code_Carbon/2021-10-27yearly_averages.csv", "mlco2/2021-10-27yearly_averages.csv"),
    ("data/owid-energy-data.csv", "owid/owid-energy-data.csv"),
    (
        "data/electricity_prices.csv",
        "global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv",
    ),
    (
        "data/world_bank_tic_exports.csv",
        "world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
    ),
    (
        "data/world_bank_metadata.csv",
        "world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
    ),
]


def _prefix_for_key(s3_key: str) -> str:
    return s3_key.split("/")[0]


def main() -> int:
    root = _repo_root()
    if load_dotenv is not None:
        load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(description="Sube CSV del proyecto al bucket S3 Bronze.")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("AWS_S3_BUCKET", "green-ai-pf-bronze-a0e96d06"),
        help="Nombre del bucket (override a env AWS_S3_BUCKET).",
    )
    parser.add_argument(
        "--only",
        choices=("reference", "usage_logs", "mlco2", "owid", "global_petrol_prices", "world_bank"),
        default=None,
        help="Solo subir un prefijo/dataset.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista acciones sin subir.",
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE"),
        help="Perfil de AWS CLI (override a env AWS_PROFILE).",
    )
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")

    todo = [
        (local, key)
        for local, key in DEFAULT_UPLOADS
        if args.only is None or _prefix_for_key(key) == args.only
    ]

    uploaded = 0
    skipped = 0

    for rel, key in todo:
        path = root / rel
        if not path.is_file():
            print(f"[omitido] no existe: {rel}", file=sys.stderr)
            skipped += 1
            continue
        uri = f"s3://{args.bucket}/{key}"
        if args.dry_run:
            print(f"[dry-run] {path} -> {uri}")
            uploaded += 1
            continue
        try:
            s3.upload_file(str(path), args.bucket, key)
            print(f"OK {uri}")
            uploaded += 1
        except (ClientError, BotoCoreError, OSError) as e:
            print(f"[error] {rel} -> {uri}: {e}", file=sys.stderr)
            return 1

    print(f"Listo: {uploaded} archivos procesados, {skipped} omitidos (no encontrados).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
