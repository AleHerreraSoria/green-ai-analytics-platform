"""
bronze_validations.py
=====================
Validaciones ligeras para la capa Bronze.

Objetivo:
  - Detectar fallos tempranos de ingesta (paths faltantes, archivos vacios,
    payloads JSON invalidados) antes de ejecutar transformaciones pesadas.

Uso:
    python bronze_validations.py
    python bronze_validations.py --fail
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import BotoCoreError, ClientError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.bronze.validations")


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


BRONZE_BUCKET = require_env("S3_BRONZE_BUCKET")


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    detail: str


@dataclass
class ValidationReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.results.append(CheckResult(name, passed, detail))

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    def print_report(self) -> None:
        sep = "=" * 70
        logger.info(sep)
        logger.info("  VALIDACION BRONZE (ligera)")
        logger.info(sep)
        for r in self.results:
            icon = "PASS" if r.passed else "FAIL"
            logger.info("[%s] %-36s %s", icon, r.check_name, r.detail)
        ok = sum(1 for r in self.results if r.passed)
        logger.info("-" * 70)
        logger.info("Resultado: %d/%d checks OK", ok, len(self.results))
        logger.info(sep)


def _prefix_has_data(s3, bucket: str, prefix: str) -> tuple[bool, str]:
    try:
        res = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if res.get("KeyCount", 0) > 0:
            return True, f"Se encontraron objetos en {prefix}"
        return False, f"Sin objetos en {prefix}"
    except (ClientError, BotoCoreError) as exc:
        return False, f"Error S3 list_objects: {exc}"


def _object_non_empty(s3, bucket: str, key: str) -> tuple[bool, str]:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        size = int(head.get("ContentLength", 0))
        if size > 0:
            return True, f"{key} size={size} bytes"
        return False, f"{key} vacio (0 bytes)"
    except (ClientError, BotoCoreError) as exc:
        return False, f"No accesible: {key} ({exc})"


def _sample_json_is_valid(s3, bucket: str, prefix: str) -> tuple[bool, str]:
    try:
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        contents = listed.get("Contents", [])
        if not contents:
            return False, f"No hay JSONs bajo {prefix}"
        key = contents[0]["Key"]
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        json.loads(body)
        return True, f"JSON valido en muestra: {key}"
    except json.JSONDecodeError as exc:
        return False, f"JSON invalido en {prefix}: {exc}"
    except (ClientError, BotoCoreError) as exc:
        return False, f"Error leyendo JSON en {prefix}: {exc}"


def main(fail_on_error: bool = False) -> None:
    s3 = boto3.client("s3")
    report = ValidationReport()

    # Checks ligeros de existencia por prefijos de ingesta.
    for prefix in [
        "usage_logs/",
        "mlco2/",
        "owid/",
        "world_bank/",
        "global_petrol_prices/",
        "electricity_maps/carbon_intensity/",
    ]:
        ok, detail = _prefix_has_data(s3, BRONZE_BUCKET, prefix)
        report.add(f"prefix_exists:{prefix}", ok, detail)

    # Checks de referencia minima (archivos criticos).
    for key in [
        "reference/geo_cloud_to_country_and_zones.csv",
        "reference/aws_ec2_on_demand_usd_per_hour.csv",
    ]:
        ok, detail = _object_non_empty(s3, BRONZE_BUCKET, key)
        report.add(f"non_empty:{key}", ok, detail)

    # Check de parseo JSON ligero en Electricity Maps.
    ok, detail = _sample_json_is_valid(
        s3,
        BRONZE_BUCKET,
        "electricity_maps/carbon_intensity/history/",
    )
    report.add("json_parse:carbon_intensity_history", ok, detail)

    report.print_report()
    if fail_on_error and not report.all_passed:
        sys.exit(1)


if __name__ == "__main__":
    main(fail_on_error="--fail" in sys.argv)
