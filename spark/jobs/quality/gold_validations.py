"""
gold_validations.py
====================
Módulo de validación de calidad para la capa Gold del proyecto Green AI.

Responsabilidades (QA Gate antes del dashboard):
  1. Integridad referencial  → region_id en fact_ai_compute_usage ⊆ dim_region
  2. Reglas de negocio       → energy_consumed_kwh >= 0, cost_electricity_usd >= 0,
                               cost_compute_usd >= 0
  3. Unicidad                → session_id estrictamente único en fact_ai_compute_usage
  4. Tablas no vacías        → cada tabla Gold debe tener al menos 1 fila
  5. Nulos en PKs/FKs        → surrogate keys no deben ser NULL

Uso:
    python gold_validations.py            # imprime reporte; no falla el proceso
    python gold_validations.py --fail     # sale con código 1 si algún check falla

Integración con el pipeline:
    Invocar al final de silver_to_gold.py (después de main()) o como
    step independiente en Airflow.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field

# ── Dependencias opcionales de desarrollo ──────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Producción: credenciales via IAM Role / Airflow env

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.gold.validations")


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value

# =============================================================================
# Constantes
# =============================================================================

GOLD_BUCKET: str = require_env("S3_GOLD_BUCKET")
GOLD: str        = f"s3a://{GOLD_BUCKET}"

# Tablas Gold completas del modelo dimensional
GOLD_TABLES: list[str] = [
    "dim_country",
    "dim_region",
    "dim_gpu_model",
    "dim_instance_type",
    "dim_electricity_price",
    "fact_ai_compute_usage",
    "fact_carbon_intensity_hourly",
    "fact_country_energy_annual",
]

# =============================================================================
# Estructura de resultados
# =============================================================================


@dataclass
class GoldCheckResult:
    """Resultado de un check de validación individual."""
    table: str
    check_name: str
    passed: bool
    detail: str
    failing_count: int = 0
    severity: str = "ERROR"   # ERROR | WARN | INFO


@dataclass
class GoldValidationReport:
    """Acumulador de todos los checks Gold."""
    results: list[GoldCheckResult] = field(default_factory=list)

    def add(self, r: GoldCheckResult) -> None:
        self.results.append(r)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == "ERROR")

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "ERROR")

    @property
    def warn_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "WARN")

    def print_report(self) -> None:
        sep = "=" * 72
        logger.info(sep)
        logger.info("  VALIDACIÓN CAPA GOLD — Green AI Analytics Platform")
        logger.info(sep)
        for r in self.results:
            icon = "✅ PASS" if r.passed else f"❌ {r.severity}"
            label = f"[{r.table}] {r.check_name}"
            logger.info("  %-8s %-52s %s", icon, label, r.detail)
        logger.info("-" * 72)
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        logger.info(
            "  Resultado: %d/%d checks pasaron | %d errores | %d advertencias",
            passed, total, self.error_count, self.warn_count,
        )
        logger.info(sep)


# =============================================================================
# Helpers genéricos
# =============================================================================


def _safe_read(spark: SparkSession, table: str) -> DataFrame | None:
    """
    Lee una tabla Gold en formato Delta.
    Retorna None (y registra el error) si el path no existe o falla la lectura.
    """
    path = f"{GOLD}/{table}"
    try:
        return spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001
        logger.error("[%s] No se pudo leer desde '%s': %s", table, path, exc)
        return None


def _check_not_empty(
    report: GoldValidationReport,
    df: DataFrame,
    table: str,
) -> bool:
    """Verifica que la tabla Gold tenga al menos 1 fila. Retorna True si OK."""
    count = df.count()
    passed = count > 0
    report.add(GoldCheckResult(
        table=table,
        check_name="tabla_no_vacía",
        passed=passed,
        detail=f"{count:,} filas encontradas",
        failing_count=0 if passed else 1,
        severity="ERROR",
    ))
    return passed


def _check_pk_not_null(
    report: GoldValidationReport,
    df: DataFrame,
    table: str,
    pk_col: str,
) -> None:
    """Verifica que la columna de clave primaria/surrogate no tenga NULLs."""
    total   = df.count()
    nulls   = df.filter(F.col(pk_col).isNull()).count()
    passed  = nulls == 0
    report.add(GoldCheckResult(
        table=table,
        check_name=f"pk_not_null({pk_col})",
        passed=passed,
        detail=f"{nulls:,} NULLs de {total:,} filas",
        failing_count=nulls,
        severity="ERROR",
    ))


def _check_non_negative(
    report: GoldValidationReport,
    df: DataFrame,
    table: str,
    col_name: str,
    severity: str = "ERROR",
) -> None:
    """
    Verifica que la columna numérica sea >= 0.
    Los NULLs se ignoran (son datos faltantes válidos, no valores negativos).
    """
    total      = df.count()
    negatives  = df.filter(
        F.col(col_name).isNotNull() & (F.col(col_name) < 0)
    ).count()
    passed = negatives == 0
    report.add(GoldCheckResult(
        table=table,
        check_name=f"{col_name} >= 0",
        passed=passed,
        detail=f"{negatives:,} valores negativos de {total:,} filas",
        failing_count=negatives,
        severity=severity,
    ))


# =============================================================================
# Checks específicos de cada tabla
# =============================================================================


def check_dim_tables(
    spark: SparkSession,
    report: GoldValidationReport,
) -> None:
    """
    Verifica que todas las tablas de dimensión existan, no estén vacías
    y que sus surrogate keys (PK) no tengan NULLs.
    """
    dim_pk_map: dict[str, str] = {
        "dim_country":           "country_id",
        "dim_region":            "region_id",
        "dim_gpu_model":         "gpu_id",
        "dim_instance_type":     "instance_type_id",
        "dim_electricity_price": "price_id",
    }

    for table, pk_col in dim_pk_map.items():
        logger.info("[%s] Verificando dimensión...", table)
        df = _safe_read(spark, table)

        if df is None:
            report.add(GoldCheckResult(
                table=table,
                check_name="tabla_accesible",
                passed=False,
                detail="No se pudo leer la tabla desde Gold.",
                failing_count=1,
                severity="ERROR",
            ))
            continue

        if not _check_not_empty(report, df, table):
            continue  # Si está vacía, los checks de PK no tienen sentido

        _check_pk_not_null(report, df, table, pk_col)


def check_referential_integrity(
    spark: SparkSession,
    report: GoldValidationReport,
) -> None:
    """
    Verifica que todos los region_id presentes en fact_ai_compute_usage
    existan en dim_region (no debe haber registros huérfanos).

    Lógica:
      orphans = fact.region_id NOT IN dim_region.region_id
      Se excluyen NULLs en fact.region_id (son sesiones sin región resuelta,
      no violaciones de FK — eso lo captura el check de nulos en FKs).
    """
    logger.info("[fact_ai_compute_usage] Verificando integridad referencial region_id → dim_region...")

    fact_df = _safe_read(spark, "fact_ai_compute_usage")
    dim_df  = _safe_read(spark, "dim_region")

    if fact_df is None or dim_df is None:
        report.add(GoldCheckResult(
            table="fact_ai_compute_usage",
            check_name="integridad_ref(region_id → dim_region)",
            passed=False,
            detail="No se pudo leer fact_ai_compute_usage o dim_region.",
            failing_count=1,
            severity="ERROR",
        ))
        return

    # Conjunto de region_ids conocidos en la dimensión
    known_regions: DataFrame = dim_df.select(
        F.col("region_id").alias("_dim_region_id")
    ).distinct()

    # Registros de hechos con region_id no nulo que NO aparecen en dim_region
    orphans: DataFrame = (
        fact_df
        .filter(F.col("region_id").isNotNull())
        .join(known_regions, fact_df["region_id"] == known_regions["_dim_region_id"], "left_anti")
    )

    orphan_count: int = orphans.count()
    total_with_region: int = fact_df.filter(F.col("region_id").isNotNull()).count()
    passed: bool = orphan_count == 0

    if not passed:
        # Muestra hasta 10 region_ids huérfanos para diagnóstico
        sample = [
            row["region_id"]
            for row in orphans.select("region_id").distinct().limit(10).collect()
        ]
        detail = (
            f"{orphan_count:,} registros huérfanos de {total_with_region:,} "
            f"con region_id no nulo. Ejemplos: {sample}"
        )
    else:
        detail = f"0 huérfanos de {total_with_region:,} registros con region_id."

    report.add(GoldCheckResult(
        table="fact_ai_compute_usage",
        check_name="integridad_ref(region_id → dim_region)",
        passed=passed,
        detail=detail,
        failing_count=orphan_count,
        severity="ERROR",
    ))


def check_fact_ai_compute_usage(
    spark: SparkSession,
    report: GoldValidationReport,
) -> None:
    """
    Checks específicos de fact_ai_compute_usage:
      1. Tabla no vacía.
      2. session_id estrictamente único (no deben existir duplicados).
      3. energy_consumed_kwh >= 0 (no puede haber consumo negativo).
      4. cost_electricity_usd >= 0 (no puede haber costo negativo).
      5. cost_compute_usd >= 0 (no puede haber costo negativo).
      6. Tasa de nulos en FKs críticas (region_id, gpu_id) ≤ 30 %.
    """
    TABLE = "fact_ai_compute_usage"
    logger.info("[%s] Verificando reglas de negocio y unicidad...", TABLE)

    df = _safe_read(spark, TABLE)
    if df is None:
        report.add(GoldCheckResult(
            table=TABLE,
            check_name="tabla_accesible",
            passed=False,
            detail="No se pudo leer la tabla desde Gold.",
            failing_count=1,
            severity="ERROR",
        ))
        return

    # 1. No vacía
    if not _check_not_empty(report, df, TABLE):
        return  # Sin datos, el resto de los checks son irrelevantes

    total: int = df.count()

    # 2. Unicidad de session_id
    distinct_sessions: int = df.select("session_id").distinct().count()
    duplicates: int = total - distinct_sessions
    report.add(GoldCheckResult(
        table=TABLE,
        check_name="unique(session_id)",
        passed=duplicates == 0,
        detail=f"{duplicates:,} session_ids duplicados de {total:,} filas",
        failing_count=duplicates,
        severity="ERROR",
    ))

    # 3. energy_consumed_kwh >= 0
    _check_non_negative(report, df, TABLE, "energy_consumed_kwh", severity="ERROR")

    # 4. cost_electricity_usd >= 0
    _check_non_negative(report, df, TABLE, "cost_electricity_usd", severity="ERROR")

    # 5. cost_compute_usd >= 0
    _check_non_negative(report, df, TABLE, "cost_compute_usd", severity="ERROR")

    # 6. Tasa de nulos en FKs clave (umbral 30 %)
    for fk_col, max_null_pct in [("region_id", 0.30), ("gpu_id", 0.50)]:
        null_count: int = df.filter(F.col(fk_col).isNull()).count()
        null_pct: float = null_count / total if total > 0 else 0.0
        passed: bool = null_pct <= max_null_pct
        report.add(GoldCheckResult(
            table=TABLE,
            check_name=f"null_rate({fk_col}) ≤ {max_null_pct:.0%}",
            passed=passed,
            detail=f"{null_pct:.1%} nulos ({null_count:,} / {total:,})",
            failing_count=null_count if not passed else 0,
            severity="WARN",
        ))


def check_fact_carbon_intensity_hourly(
    spark: SparkSession,
    report: GoldValidationReport,
) -> None:
    """
    Checks de fact_carbon_intensity_hourly:
      1. Tabla no vacía.
      2. carbon_intensity_gco2eq_per_kwh en rango [0, 1500] gCO₂eq/kWh.
         (el máximo teórico de una red 100 % carbón ~1100, 1500 es margen conservador)
      3. zone no nula.
    """
    TABLE = "fact_carbon_intensity_hourly"
    logger.info("[%s] Verificando reglas de negocio...", TABLE)

    df = _safe_read(spark, TABLE)
    if df is None:
        report.add(GoldCheckResult(
            table=TABLE, check_name="tabla_accesible", passed=False,
            detail="No se pudo leer la tabla desde Gold.",
            failing_count=1, severity="ERROR",
        ))
        return

    if not _check_not_empty(report, df, TABLE):
        return

    total: int = df.count()

    # Rango de intensidad de carbono
    out_of_range: int = df.filter(
        F.col("carbon_intensity_gco2eq_per_kwh").isNotNull() &
        (
            (F.col("carbon_intensity_gco2eq_per_kwh") < 0) |
            (F.col("carbon_intensity_gco2eq_per_kwh") > 1500)
        )
    ).count()
    report.add(GoldCheckResult(
        table=TABLE,
        check_name="carbon_intensity en [0, 1500] gCO₂eq/kWh",
        passed=out_of_range == 0,
        detail=f"{out_of_range:,} valores fuera de rango de {total:,}",
        failing_count=out_of_range,
        severity="ERROR",
    ))

    # zone no nula
    null_zone: int = df.filter(F.col("zone").isNull()).count()
    report.add(GoldCheckResult(
        table=TABLE,
        check_name="zone not null",
        passed=null_zone == 0,
        detail=f"{null_zone:,} filas sin zona de {total:,}",
        failing_count=null_zone,
        severity="ERROR",
    ))


def check_fact_country_energy_annual(
    spark: SparkSession,
    report: GoldValidationReport,
) -> None:
    """
    Checks de fact_country_energy_annual:
      1. Tabla no vacía.
      2. year en rango [1990, 2030].
      3. gdp_per_capita >= 0 (cuando no es NULL).
      4. Unicidad del par (country_id, year).
    """
    TABLE = "fact_country_energy_annual"
    logger.info("[%s] Verificando reglas de negocio...", TABLE)

    df = _safe_read(spark, TABLE)
    if df is None:
        report.add(GoldCheckResult(
            table=TABLE, check_name="tabla_accesible", passed=False,
            detail="No se pudo leer la tabla desde Gold.",
            failing_count=1, severity="ERROR",
        ))
        return

    if not _check_not_empty(report, df, TABLE):
        return

    total: int = df.count()

    # Rango de año
    bad_year: int = df.filter(
        F.col("year").isNull() |
        (F.col("year") < 1990) |
        (F.col("year") > 2030)
    ).count()
    report.add(GoldCheckResult(
        table=TABLE,
        check_name="year en [1990, 2030]",
        passed=bad_year == 0,
        detail=f"{bad_year:,} años fuera de rango de {total:,}",
        failing_count=bad_year,
        severity="ERROR",
    ))

    # gdp_per_capita >= 0
    _check_non_negative(report, df, TABLE, "gdp_per_capita", severity="WARN")

    # Unicidad (country_id, year)
    distinct_pairs: int = df.select("country_id", "year").distinct().count()
    dup_pairs: int = total - distinct_pairs
    report.add(GoldCheckResult(
        table=TABLE,
        check_name="unique(country_id, year)",
        passed=dup_pairs == 0,
        detail=f"{dup_pairs:,} pares (country_id, year) duplicados de {total:,}",
        failing_count=dup_pairs,
        severity="ERROR",
    ))


# =============================================================================
# Función principal de validación (entry point para el pipeline)
# =============================================================================


def validate_gold_quality(spark: SparkSession) -> GoldValidationReport:
    """
    Orquestador de todos los checks Gold.

    Ejecuta los validadores en el orden:
      1. Dimensiones      → existencia, no vacías, PK no nulas.
      2. Integridad ref.  → FK region_id de fact_ai_compute_usage → dim_region.
      3. Hechos           → reglas de negocio, unicidad, rangos.

    Parámetros
    ----------
    spark : SparkSession
        Sesión Spark ya inicializada con acceso a S3.

    Retorna
    -------
    GoldValidationReport
        Objeto con todos los resultados; consultar `.all_passed` para el status global.
    """
    report = GoldValidationReport()

    validators = [
        # ── Dimensiones ───────────────────────────────────────────────────────
        lambda r: check_dim_tables(spark, r),
        # ── Integridad Referencial ────────────────────────────────────────────
        lambda r: check_referential_integrity(spark, r),
        # ── Tablas de Hechos ──────────────────────────────────────────────────
        lambda r: check_fact_ai_compute_usage(spark, r),
        lambda r: check_fact_carbon_intensity_hourly(spark, r),
        lambda r: check_fact_country_energy_annual(spark, r),
    ]

    for validator in validators:
        try:
            validator(report)
        except Exception as exc:  # noqa: BLE001
            logger.error("Excepción inesperada en un validador Gold: %s", exc, exc_info=True)
            report.add(GoldCheckResult(
                table="UNKNOWN",
                check_name="validator_execution",
                passed=False,
                detail=f"Excepción inesperada: {exc}",
                failing_count=1,
                severity="ERROR",
            ))

    report.print_report()
    return report


# =============================================================================
# Inicialización de Spark (standalone — reutiliza sesión existente si hay una)
# =============================================================================


def _get_or_create_spark() -> SparkSession:
    """
    Obtiene la SparkSession activa o crea una nueva con configuración mínima
    para conectarse a S3 en modo lectura (validación solamente).
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        logger.info("Reutilizando SparkSession activa.")
        return existing

    logger.info("Creando nueva SparkSession para validación Gold...")
    return (
        SparkSession.builder
        .appName("green-ai-gold-validations")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "software.amazon.awssdk:bundle:2.20.18",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
        .config("spark.hadoop.fs.s3a.multipart.purge",              "false")
        .config("spark.hadoop.fs.s3a.multipart.purge.age",          "86400")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


# =============================================================================
# Entry Point CLI
# =============================================================================

if __name__ == "__main__":
    fail_on_error: bool = "--fail" in sys.argv

    _spark = _get_or_create_spark()
    _spark.sparkContext.setLogLevel("WARN")

    _report = validate_gold_quality(_spark)

    _spark.stop()
    logger.info("SparkSession cerrada.")

    if fail_on_error and not _report.all_passed:
        logger.error(
            "Pipeline abortado: %d check(s) ERROR fallaron.",
            _report.error_count,
        )
        sys.exit(1)
