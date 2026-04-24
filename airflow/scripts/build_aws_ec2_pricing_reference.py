"""
Genera `reference/aws_ec2_on_demand_usd_per_hour.csv` directo en S3 Bronze (sin disco local).

Precios base (USD/h, Linux, On-Demand, shared tenancy) para US East (N. Virginia)
tomados de referencia pública https://instances.vantage.sh (consulta 2026-04).
Multiplicadores regionales son aproximaciones frecuentes respecto a us-east-1;
reemplazar por AWS Price List API o export oficial para producción.

Lee insumos desde el mismo bucket Bronze:
- reference/geo_cloud_to_country_and_zones.csv
- mlco2/instances.csv
"""

from __future__ import annotations

import csv
import io
import os
import sys
from typing import Any

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Instala boto3: pip install boto3", file=sys.stderr)
    raise SystemExit(1)

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


def _read_csv_from_s3(client: Any, bucket: str, key: str) -> list[dict[str, str]]:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
    except (ClientError, BotoCoreError, OSError, KeyError) as e:
        raise RuntimeError(f"No se pudo leer s3://{bucket}/{key}: {e}") from e
    return list(csv.DictReader(io.StringIO(body)))


def _load_aws_regions(client: Any, bucket: str, key: str) -> list[str]:
    rows = _read_csv_from_s3(client, bucket, key)
    return [row["cloud_region"] for row in rows if row.get("cloud_provider") == "aws"]


def _load_aws_instance_types(client: Any, bucket: str, key: str) -> list[str]:
    rows = _read_csv_from_s3(client, bucket, key)
    out: list[str] = []
    for row in rows:
        if (row.get("provider") or "").strip().lower() == "aws":
            iid = (row.get("id") or "").strip()
            if iid:
                out.append(iid)
    return out


def main() -> int:
    bucket = (os.environ.get("S3_BRONZE_BUCKET") or "").strip()
    if not bucket:
        print("Falta S3_BRONZE_BUCKET.", file=sys.stderr)
        return 1

    region_name = os.environ.get("AWS_DEFAULT_REGION") or None
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None)
    s3 = session.client("s3", region_name=region_name)

    regions = _load_aws_regions(s3, bucket, "reference/geo_cloud_to_country_and_zones.csv")
    instances = _load_aws_instance_types(s3, bucket, "mlco2/instances.csv")
    missing_base = [i for i in instances if i not in _BASE_US_EAST_1]
    if missing_base:
        print(f"Faltan precios base us-east-1 para: {missing_base}", file=sys.stderr)
        return 1
    missing_mult = [reg for reg in regions if reg not in _REGION_MULT]
    if missing_mult:
        print(f"Faltan multiplicadores regionales para: {missing_mult}", file=sys.stderr)
        return 1

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
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=fieldnames)
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

    key = "reference/aws_ec2_on_demand_usd_per_hour.csv"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=out.getvalue().encode("utf-8"),
            ContentType="text/csv; charset=utf-8",
        )
    except (ClientError, BotoCoreError, OSError) as e:
        print(f"No se pudo escribir s3://{bucket}/{key}: {e}", file=sys.stderr)
        return 1

    print(f"OK s3://{bucket}/{key} ({rows_written} filas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
