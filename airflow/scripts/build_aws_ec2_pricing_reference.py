"""
Genera `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv`.

Precios base (USD/h, Linux, On-Demand, shared tenancy) para US East (N. Virginia)
tomados de referencia pública https://instances.vantage.sh (consulta 2026-04).
Multiplicadores regionales son aproximaciones frecuentes respecto a us-east-1;
reemplazar por AWS Price List API o export oficial para producción.

Requisitos: solo stdlib. Lee regiones de `bronze/reference/geo_cloud_to_country_and_zones.csv`
e instancias de `data/Code_Carbon/instances.csv` (provider=aws).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

# USD/h en us-east-1 — Linux, On-Demand (Vantage / listado público EC2, abril 2026)
_BASE_US_EAST_1: dict[str, float] = {
    "p3.2xlarge": 3.06,
    "p3.8xlarge": 12.24,
    "p3.16xlarge": 24.48,
    "p3dn.24xlarge": 31.212,
    "p2.xlarge": 0.90,
    "p2.8xlarge": 7.20,
    "p2.16xlarge": 14.40,
    "g4dn.xlarge": 0.526,
    "g4dn.2xlarge": 0.752,
    "g4dn.4xlarge": 1.204,
    "g4dn.8xlarge": 2.176,
    "g4dn.12xlarge": 3.912,
    "g4dn.16xlarge": 4.352,
    "g4dn.metal": 7.824,
}

# Factor vs us-east-1 mismo SKU (aproximación; Sao Paulo y Gov suelen marcar más)
_REGION_MULT: dict[str, float] = {
    "us-east-1": 1.0,
    "us-east-2": 1.0,
    "us-west-1": 1.05,
    "us-west-2": 1.0,
    "ap-east-1": 1.09,
    "ap-south-1": 1.04,
    "ap-northeast-3": 1.14,
    "ap-northeast-2": 1.10,
    "ap-southeast-1": 1.10,
    "ap-southeast-2": 1.12,
    "ap-northeast-1": 1.14,
    "ca-central-1": 1.02,
    "cn-north-1": 1.0,
    "cn-northwest-1": 1.0,
    "eu-central-1": 1.05,
    "eu-west-1": 1.08,
    "eu-west-2": 1.09,
    "eu-west-3": 1.05,
    "eu-north-1": 0.98,
    "sa-east-1": 1.52,
    "us-gov-east-1": 1.15,
    "us-gov-west-1": 1.15,
}

_AS_OF = "2026-04-08"
_SOURCE = "instances.vantage.sh us-east-1 OD Linux + regional multipliers (approx; see script)"


def _spark_root() -> Path:
    # .../spark/jobs/reference/build_aws_ec2_pricing_reference.py -> .../spark
    return Path(__file__).resolve().parents[2]


def _repo_root() -> Path:
    # .../repo/spark/jobs/reference/build_aws_ec2_pricing_reference.py -> .../repo
    return Path(__file__).resolve().parents[3]


def _first_existing(candidates: list[Path], label: str) -> Path:
    for p in candidates:
        if p.exists():
            return p
    joined = "\n".join(f"- {c}" for c in candidates)
    raise FileNotFoundError(f"No se encontró {label}. Rutas intentadas:\n{joined}")


def _load_aws_regions(spark_root: Path, repo_root: Path) -> list[str]:
    path = _first_existing(
        [
            spark_root / "bronze/reference/geo_cloud_to_country_and_zones.csv",
            repo_root / "bronze/reference/geo_cloud_to_country_and_zones.csv",
            repo_root / "data/geo_cloud_to_country_and_zones.csv",
        ],
        "geo_cloud_to_country_and_zones.csv",
    )
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return [row["cloud_region"] for row in r if row.get("cloud_provider") == "aws"]


def _load_aws_instance_types(spark_root: Path, repo_root: Path) -> list[str]:
    path = _first_existing(
        [
            repo_root / "data/Code_Carbon/instances.csv",
            spark_root / "data/Code_Carbon/instances.csv",
        ],
        "instances.csv de Code Carbon",
    )
    out: list[str] = []
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("provider") or "").strip().lower() == "aws":
                out.append(row["id"].strip())
    return out


def main() -> int:
    spark_root = _spark_root()
    repo_root = _repo_root()
    regions = _load_aws_regions(spark_root, repo_root)
    instances = _load_aws_instance_types(spark_root, repo_root)
    missing_base = [i for i in instances if i not in _BASE_US_EAST_1]
    if missing_base:
        print(f"Faltan precios base us-east-1 para: {missing_base}", file=sys.stderr)
        return 1
    missing_mult = [reg for reg in regions if reg not in _REGION_MULT]
    if missing_mult:
        print(f"Faltan multiplicadores regionales para: {missing_mult}", file=sys.stderr)
        return 1

    out_path = spark_root / "bronze/reference/aws_ec2_on_demand_usd_per_hour.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "cloud_provider",
        "cloud_region",
        "instance_type",
        "operating_system",
        "tenancy",
        "pricing_model",
        "currency",
        "price_usd_per_hour",
        "price_basis_region",
        "regional_multiplier",
        "as_of_date",
        "source",
        "pricing_notes",
    ]
    rows_written = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for region in regions:
            mult = _REGION_MULT[region]
            for itype in instances:
                base = _BASE_US_EAST_1[itype]
                price = round(base * mult, 4)
                w.writerow(
                    {
                        "cloud_provider": "aws",
                        "cloud_region": region,
                        "instance_type": itype,
                        "operating_system": "linux",
                        "tenancy": "shared",
                        "pricing_model": "on_demand",
                        "currency": "USD",
                        "price_usd_per_hour": f"{price:.4f}",
                        "price_basis_region": "us-east-1",
                        "regional_multiplier": f"{mult:.4f}",
                        "as_of_date": _AS_OF,
                        "source": _SOURCE,
                        "pricing_notes": "TCO compute proxy; excludes EBS egress RI Spot. China/Gov multipliers approximate.",
                    }
                )
                rows_written += 1

    print(f"OK {out_path} ({rows_written} filas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
