"""
Sube datasets fuente al bucket "sources".

Objetivo:
- Cargar a S3 todos los archivos dentro de `data/` excepto los generados por pipeline.
- Subir el contenido de `data/` directamente en la raíz del bucket (sin prefijo `data/`).
- Cargar además el archivo de referencia geo_cloud_to_country_and_zones.csv.

Variables:
- Bucket destino: AWS_S3_SOURCES_BUCKET (o --bucket)
- Región: AWS_DEFAULT_REGION
- Perfil AWS CLI: AWS_PROFILE (o --profile)
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


EXCLUDED_DATA_FILES = {
    "aws_ec2_on_demand_scd_per_hour.csv",
    "usage_logs.csv",
}

EXCLUDED_DATA_RELATIVE_PATHS = {
    "reference/geo_cloud_to_country_and_zones.csv",
}

EXCLUDED_DATA_PREFIXES = (
    "bronze/",
)

REFERENCE_UPLOADS: list[tuple[str, str]] = [
    (
        "data/reference/geo_cloud_to_country_and_zones.csv",
        "reference/geo_cloud_to_country_and_zones.csv",
    ),
]


def _repo_root() -> Path:
    # upload_sources_to_s3.py -> scripts -> airflow -> repo_root
    return Path(__file__).resolve().parents[2]


def _build_uploads(root: Path) -> list[tuple[Path, str]]:
    uploads: list[tuple[Path, str]] = []

    data_dir = root / "data"
    if data_dir.is_dir():
        for path in data_dir.rglob("*"):
            if not path.is_file():
                continue
            relative_key = path.relative_to(data_dir).as_posix()
            if (
                path.name in EXCLUDED_DATA_FILES
                or relative_key in EXCLUDED_DATA_RELATIVE_PATHS
                or any(relative_key.startswith(prefix) for prefix in EXCLUDED_DATA_PREFIXES)
            ):
                continue
            uploads.append((path, relative_key))

    for local_rel, s3_key in REFERENCE_UPLOADS:
        uploads.append((root / local_rel, s3_key))

    # Orden estable para reproducibilidad en logs
    uploads.sort(key=lambda item: item[1])
    return uploads


def main() -> int:
    root = _repo_root()
    if load_dotenv is not None:
        load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(
        description="Sube datasets desde data/ al bucket S3 sources."
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("AWS_S3_SOURCES_BUCKET", "sources"),
        help="Nombre del bucket destino (override a env AWS_S3_SOURCES_BUCKET).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista acciones sin subir.",
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE"),
        help="Perfil AWS CLI (override a env AWS_PROFILE).",
    )
    args = parser.parse_args()

    uploads = _build_uploads(root)
    if not uploads:
        print("[info] No se encontraron archivos para subir.")
        return 0

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")

    uploaded = 0
    skipped = 0

    for local_path, key in uploads:
        if not local_path.is_file():
            print(f"[omitido] no existe: {local_path.relative_to(root).as_posix()}", file=sys.stderr)
            skipped += 1
            continue

        uri = f"s3://{args.bucket}/{key}"
        if args.dry_run:
            print(f"[dry-run] {local_path} -> {uri}")
            uploaded += 1
            continue

        try:
            s3.upload_file(str(local_path), args.bucket, key)
            print(f"OK {uri}")
            uploaded += 1
        except (ClientError, BotoCoreError, OSError) as err:
            rel = local_path.relative_to(root).as_posix()
            print(f"[error] {rel} -> {uri}: {err}", file=sys.stderr)
            return 1

    print(f"Listo: {uploaded} archivos procesados, {skipped} omitidos (no encontrados).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
