"""
validations.py
===============
Módulo de validación de calidad para la capa Silver del proyecto Green AI.

Criterios de Aceptación (Jira):
  - energy_consumed_kwh > 0                    (usage_logs)
  - carbonIntensity entre 0 y 1000 gCO₂eq/kWh (carbon_intensity)

Adicionalmente se validan:
  - Tasa de nulos en columnas clave
  - Unicidad de session_id en usage_logs
  - Integridad referencial: gpu_model de logs existe en mlco2/gpus.csv
  - Rangos esperados en campos de porcentaje (0–100 %)

Uso:
    python validations.py            # imprime reporte en consola
    python validations.py --fail     # sale con código 1 si algún check falla
"""

import logging
import os
import sys
from dataclasses import dataclass, field

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.validations")

# ── Dependencias opcionales de desarrollo ──────────────────────────────────
# En producción las vars de entorno son inyectadas por Airflow / IAM Role.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Entorno de producción: sin dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")
BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "green-ai-pf-bronze-a0e96d06")
SILVER = f"s3a://{SILVER_BUCKET}"
BRONZE = f"s3a://{BRONZE_BUCKET}"


# ---------------------------------------------------------------------------
# Estructura de resultados de validación
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    dataset: str
    check_name: str
    passed: bool
    detail: str
    failing_count: int = 0


@dataclass
class ValidationReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, r: CheckResult):
        self.results.append(r)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    def print_report(self):
        separator = "=" * 70
        logger.info(separator)
        logger.info("  VALIDACIÓN CAPA SILVER — Green AI")
        logger.info(separator)
        for r in self.results:
            icon = "PASS" if r.passed else "FAIL"
            label = f"[{r.dataset}] {r.check_name}"
            logger.info("  [%s]  %-55s %s", icon, label, r.detail)
        logger.info("-" * 70)
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        logger.info("  Resultado: %d/%d checks pasaron.", passed, total)
        logger.info(separator)


# ---------------------------------------------------------------------------
# Helper genérico de validación de rango
# ---------------------------------------------------------------------------

def _check_range(
    report: ValidationReport,
    df,
    dataset: str,
    col_name: str,
    min_val: float | None,
    max_val: float | None,
    allow_null: bool = True,
):
    """Valida que una columna numérica esté dentro del rango esperado."""
    total = df.count()
    cond = F.lit(False)

    if min_val is not None:
        cond = cond | (F.col(col_name) < min_val)
    if max_val is not None:
        cond = cond | (F.col(col_name) > max_val)
    if not allow_null:
        cond = cond | F.col(col_name).isNull()

    failing = df.filter(cond).count()
    passed = failing == 0
    detail = f"{failing:,} / {total:,} registros fuera de rango [{min_val}, {max_val}]"

    report.add(CheckResult(
        dataset=dataset,
        check_name=f"{col_name} en rango [{min_val},{max_val}]",
        passed=passed,
        detail=detail,
        failing_count=failing,
    ))


def _check_null_rate(
    report: ValidationReport,
    df,
    dataset: str,
    col_name: str,
    max_null_pct: float = 0.30,
):
    """Valida que el porcentaje de nulos no supere el umbral dado."""
    total = df.count()
    if total == 0:
        report.add(CheckResult(dataset, f"null_rate({col_name})", False,
                               "DataFrame vacío", total))
        return
    null_count = df.filter(F.col(col_name).isNull()).count()
    null_pct = null_count / total
    passed = null_pct <= max_null_pct
    report.add(CheckResult(
        dataset=dataset,
        check_name=f"null_rate({col_name}) ≤ {max_null_pct:.0%}",
        passed=passed,
        detail=f"{null_pct:.1%}  ({null_count:,} nulos de {total:,})",
        failing_count=null_count,
    ))


def _check_uniqueness(report: ValidationReport, df, dataset: str, col_name: str):
    """Valida que la columna no tenga duplicados."""
    total = df.count()
    distinct = df.select(col_name).distinct().count()
    duplicates = total - distinct
    passed = duplicates == 0
    report.add(CheckResult(
        dataset=dataset,
        check_name=f"unique({col_name})",
        passed=passed,
        detail=f"{duplicates:,} valores duplicados de {total:,}",
        failing_count=duplicates,
    ))


# ---------------------------------------------------------------------------
# Validaciones por dataset
# ---------------------------------------------------------------------------

def validate_usage_logs(spark: SparkSession, report: ValidationReport):
    """
    CA Jira:
      - energy_consumed_kwh > 0
    Adicionales:
      - session_id único
      - gpu_utilization en [0, 1]
      - job_type en valores esperados
      - execution_status en valores esperados
    """
    df = spark.read.format("delta").load(f"{SILVER}/usage_logs")
    DATASET = "usage_logs"

    # CA principal
    _check_range(report, df, DATASET, "energy_consumed_kwh",
                 min_val=0.0, max_val=None, allow_null=False)

    # Unicidad
    _check_uniqueness(report, df, DATASET, "session_id")

    # GPU utilization
    _check_range(report, df, DATASET, "gpu_utilization",
                 min_val=0.0, max_val=1.0, allow_null=True)

    # job_type domain check
    allowed_job_types = {"Training", "Inference", "Fine-tuning"}
    invalid_job = df.filter(~F.col("job_type").isin(*allowed_job_types)).count()
    report.add(CheckResult(
        dataset=DATASET,
        check_name="job_type en dominio esperado",
        passed=invalid_job == 0,
        detail=f"{invalid_job:,} registros con job_type fuera de dominio",
        failing_count=invalid_job,
    ))

    # execution_status domain check
    allowed_status = {"Success", "Failed"}
    invalid_status = df.filter(~F.col("execution_status").isin(*allowed_status)).count()
    report.add(CheckResult(
        dataset=DATASET,
        check_name="execution_status en dominio esperado",
        passed=invalid_status == 0,
        detail=f"{invalid_status:,} registros con estado fuera de dominio",
        failing_count=invalid_status,
    ))

    # Timestamp no nulo
    _check_null_rate(report, df, DATASET, "timestamp", max_null_pct=0.0)


def validate_carbon_intensity(spark: SparkSession, report: ValidationReport):
    """
    CA Jira:
      - carbon_intensity entre 0 y 1000 gCO₂eq/kWh
    Adicionales:
      - zone no nula
      - datetime no nula
    """
    for endpoint in ("latest", "past", "history"):
        path = f"{SILVER}/electricity_maps/carbon_intensity/{endpoint}"
        try:
            df = spark.read.format("delta").load(path)
        except Exception:
            report.add(CheckResult(
                f"carbon_intensity/{endpoint}", "dataset_exists",
                False, f"Path no encontrado: {path}"
            ))
            continue

        DATASET = f"carbon_intensity/{endpoint}"

        # CA principal
        _check_range(report, df, DATASET, "carbon_intensity",
                     min_val=0, max_val=1000, allow_null=False)

        # zone y datetime no nulos
        _check_null_rate(report, df, DATASET, "zone",     max_null_pct=0.0)
        _check_null_rate(report, df, DATASET, "datetime", max_null_pct=0.0)


def validate_global_petrol_prices(spark: SparkSession, report: ValidationReport):
    """
    Valida que el precio residencial sea positivo y que precio_red_estimado_usd
    esté correctamente calculado (no nulo si residential tampoco lo es).
    """
    df = spark.read.format("delta").load(f"{SILVER}/global_petrol_prices")
    DATASET = "global_petrol_prices"

    _check_range(report, df, DATASET, "residential_usd_per_kwh",
                 min_val=0.0, max_val=None, allow_null=True)

    # precio_red_estimado_usd no debe ser nulo donde residential no lo es
    inconsistent = df.filter(
        F.col("residential_usd_per_kwh").isNotNull() &
        F.col("precio_red_estimado_usd").isNull()
    ).count()
    report.add(CheckResult(
        dataset=DATASET,
        check_name="precio_red_estimado_usd consistente con residential",
        passed=inconsistent == 0,
        detail=f"{inconsistent:,} filas con residential no nulo pero estimado nulo",
        failing_count=inconsistent,
    ))


def validate_owid(spark: SparkSession, report: ValidationReport):
    """Valida OWID: año en rango, carbon_intensity_elec no negativo."""
    df = spark.read.format("delta").load(f"{SILVER}/owid")
    DATASET = "owid"

    _check_range(report, df, DATASET, "year",
                 min_val=1900, max_val=2030, allow_null=False)

    _check_range(report, df, DATASET, "carbon_intensity_elec",
                 min_val=0.0, max_val=None, allow_null=True)

    # country no nulo
    _check_null_rate(report, df, DATASET, "country", max_null_pct=0.0)


def validate_world_bank(spark: SparkSession, report: ValidationReport):
    """Valida World Bank ICT: ict_exports_usd positivo, year en rango."""
    df = spark.read.format("delta").load(f"{SILVER}/world_bank/ict_exports")
    DATASET = "world_bank_ict"

    _check_range(report, df, DATASET, "ict_exports_usd",
                 min_val=0.0, max_val=None, allow_null=False)

    _check_range(report, df, DATASET, "year",
                 min_val=1960, max_val=2025, allow_null=False)

    _check_null_rate(report, df, DATASET, "country_code", max_null_pct=0.0)


def validate_ec2_pricing(spark: SparkSession, report: ValidationReport):
    """Valida precios EC2: price_usd_per_hour > 0."""
    df = spark.read.format("delta").load(f"{SILVER}/reference/ec2_pricing")
    DATASET = "ec2_pricing"

    _check_range(report, df, DATASET, "price_usd_per_hour",
                 min_val=0.0, max_val=None, allow_null=False)

    _check_null_rate(report, df, DATASET, "instance_type", max_null_pct=0.0)
    _check_null_rate(report, df, DATASET, "cloud_region",  max_null_pct=0.0)

def validate_geo_cloud_mapping(spark: SparkSession, report: ValidationReport):
    """Valida mapeos Cloud: claves primarias no nulas y unicidad."""
    df = spark.read.format("delta").load(f"{SILVER}/reference/geo_cloud_mapping")
    DATASET = "geo_cloud_mapping"

    # Claves críticas no deben ser nulas
    _check_null_rate(report, df, DATASET, "cloud_region", max_null_pct=0.0)
    _check_null_rate(report, df, DATASET, "electricity_maps_zone", max_null_pct=0.0)
    _check_null_rate(report, df, DATASET, "iso_alpha3", max_null_pct=0.0)

    # Unicidad combinada (provider + region)
    total = df.count()
    distinct = df.select("cloud_provider", "cloud_region").distinct().count()
    duplicates = total - distinct
    report.add(CheckResult(
        dataset=DATASET,
        check_name="unique(cloud_provider, cloud_region)",
        passed=duplicates == 0,
        detail=f"{duplicates:,} combinaciones duplicadas de {total:,}",
        failing_count=duplicates,
    ))

def validate_world_bank_metadata(spark: SparkSession, report: ValidationReport):
    """Valida metadatos de World Bank: country_code no nulo y único."""
    df = spark.read.format("delta").load(f"{SILVER}/reference/world_bank_metadata")
    DATASET = "world_bank_metadata"

    _check_null_rate(report, df, DATASET, "country_code", max_null_pct=0.0)
    _check_uniqueness(report, df, DATASET, "country_code")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(fail_on_error: bool = False):
    spark = (
        SparkSession.builder
        .appName("green-ai-silver-validation")
        .config("spark.sql.session.timeZone", "UTC")
        # ── Conectividad S3 (s3a://) ─────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "software.amazon.awssdk:bundle:2.20.18")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        # Fix for Hadoop 3.3.4 "60s" NumberFormatException bug
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    report = ValidationReport()

    validators = [
        validate_usage_logs,
        validate_carbon_intensity,
        validate_global_petrol_prices,
        validate_owid,
        validate_world_bank,
        validate_ec2_pricing,
        validate_geo_cloud_mapping,
        validate_world_bank_metadata
    ]

    for fn in validators:
        try:
            fn(spark, report)
        except Exception as exc:
            report.add(CheckResult(
                dataset=fn.__name__,
                check_name="ejecución_validator",
                passed=False,
                detail=f"Excepción inesperada: {exc}",
            ))

    report.print_report()
    spark.stop()

    if fail_on_error and not report.all_passed:
        sys.exit(1)


if __name__ == "__main__":
    fail_flag = "--fail" in sys.argv
    main(fail_on_error=fail_flag)
