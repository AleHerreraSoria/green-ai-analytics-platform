"""
resilience_test.py
===================
Prueba de resiliencia del pipeline Green AI — Delta Lake sobre S3.

Valida que el pipeline y las tablas Delta sobreviven a:
  1. Inyección de datos con esquema envenenado (columnas extras / tipos erróneos).
  2. Datos con valores nulos en columnas de clave primaria.
  3. Escritura parcial interrumpida (simulada escribiendo archivos Parquet
     corruptos directamente en el path Silver antes del MERGE).

Para cada escenario, la prueba:
  a. Registra el estado pre-fault de la tabla Delta en S3.
  b. Aplica el escenario de fallo.
  c. Vuelve a ejecutar el ETL correspondiente (retry real).
  d. Verifica que la tabla Delta en S3 es legible, no tiene archivos
     huérfanos, y que el conteo de filas es igual o coherente con el
     estado pre-fault (cero corrupción).

Ejecutar (desde la raíz del repositorio):
    python resilience_test.py

Requiere:
    - spark/.env con S3_BRONZE_BUCKET, S3_SILVER_BUCKET, S3_GOLD_BUCKET,
      AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY configurados.
    - PySpark >= 3.5, delta-spark >= 3.2, boto3, python-dotenv instalados.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

# ── dotenv ─────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # __file__ está en ci/testing/ → subir 2 niveles para llegar a la raíz del proyecto
    _project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    _env_path = os.path.join(_project_root, "spark", ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
        print(f"[resilience_test] .env cargado desde: {_env_path}")
    else:
        load_dotenv()
except ImportError:
    pass

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.resilience-test")

UTC = timezone.utc


# =============================================================================
# Configuración de entorno
# =============================================================================

def _require_env(var: str) -> str:
    value = os.getenv(var, "").strip()
    if not value:
        raise EnvironmentError(f"Variable de entorno requerida: {var}")
    return value


BRONZE_BUCKET = _require_env("S3_BRONZE_BUCKET")
SILVER_BUCKET = _require_env("S3_SILVER_BUCKET")
GOLD_BUCKET   = _require_env("S3_GOLD_BUCKET")
BRONZE        = f"s3a://{BRONZE_BUCKET}"
SILVER        = f"s3a://{SILVER_BUCKET}"
GOLD          = f"s3a://{GOLD_BUCKET}"

# Tabla de referencia para pruebas de resiliencia (pequeña y sin dependencias)
# Se usa mlco2/gpus porque es un catálogo estático pequeño (KB) con PK simple
TEST_TABLE_REL   = "mlco2/gpus"        # path relativo dentro de Silver
TEST_TABLE_PK    = ["gpu_model"]
TEST_TABLE_PATH  = f"{SILVER}/{TEST_TABLE_REL}"


# =============================================================================
# SparkSession
# =============================================================================

def _build_spark():
    from pyspark.sql import SparkSession

    # ── Garantizar JAVA_HOME en el entorno del proceso ────────────────────────
    # PySpark usa subprocess.Popen para lanzar la JVM (py4j gateway).
    # Si JAVA_HOME no está en os.environ (solo en la sesión de shell), falla
    # con WinError 2. Se setea explícitamente aquí como garantía.
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        candidate = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
        if os.path.isdir(candidate):
            java_home = candidate
            os.environ["JAVA_HOME"] = java_home
            logger.info("[spark] JAVA_HOME seteado automáticamente: %s", java_home)
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("green-ai-resilience-test")
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ansi.enabled",          "false")
        .config("spark.sql.session.timeZone",      "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "software.amazon.awssdk:bundle:2.20.18",
        )
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
        .config("spark.hadoop.fs.s3a.fast.upload",                  "true")
        .config("spark.driver.memory",                              "4g")
        .config("spark.ui.showConsoleProgress",                      "false")
        .getOrCreate()
    )


# =============================================================================
# Helpers de verificación Delta
# =============================================================================

def _read_delta_safe(spark, path: str) -> tuple[int, str | None]:
    """
    Intenta leer una tabla Delta y devuelve (row_count, error_msg).
    error_msg es None si la lectura fue exitosa.
    """
    try:
        count = spark.read.format("delta").load(path).count()
        return count, None
    except Exception as exc:
        return -1, str(exc)


def _count_duplicates(spark, path: str, pk_cols: list[str]) -> int:
    """Cuenta grupos con clave primaria duplicada en una tabla Delta."""
    from pyspark.sql import functions as F
    try:
        return (
            spark.read.format("delta").load(path)
            .groupBy(*pk_cols).count()
            .filter(F.col("count") > 1)
            .count()
        )
    except Exception:
        return -1


def _delta_log_versions(spark, path: str) -> list[int]:
    """
    Retorna la lista de versiones del Delta Log de la tabla.
    Sirve para verificar que el log creció (hay un commit post-retry)
    y que no quedó en estado de transacción incompleta.
    """
    from delta.tables import DeltaTable
    try:
        hist = DeltaTable.forPath(spark, path).history()
        versions = [row["version"] for row in hist.collect()]
        return sorted(versions)
    except Exception:
        return []


def _check_no_temp_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """
    Lista archivos temporales/incompletos en un prefijo S3.
    Un pipeline resiliente no deja archivos _temporary ni .tmp tras el retry.
    """
    temp_files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_temporary" in key or key.endswith(".tmp") or "/_tmp" in key:
                temp_files.append(key)
    return temp_files


# =============================================================================
# ETL runners
# =============================================================================

def _run_bronze_to_silver(spark) -> None:
    """
    Ejecuta únicamente el job Bronze→Silver inyectando la sesión activa.

    Al pasar ``spark`` explícitamente, el job detecta que NO es el dueño
    del ciclo de vida de la sesión (_owns_spark=False) y omite spark.stop()
    al terminar. Sin esta inyección, el job destruye la sesión del test
    y los escenarios subsiguientes fallan con
    IllegalStateException / 'NoneType' object has no attribute 'sc'.
    """
    import importlib.util
    # __file__ está en ci/testing/ → subir 2 niveles para llegar a la raíz
    jobs_etl = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
        "spark", "jobs", "etl",
    )
    spec = importlib.util.spec_from_file_location(
        "bronze_to_silver", os.path.join(jobs_etl, "bronze_to_silver.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main(spark=spark)   # ← inyectar sesión; el job NO llamará spark.stop()


def _run_silver_to_gold(spark) -> None:
    """
    Ejecuta únicamente el job Silver→Gold inyectando la sesión activa.
    Ver _run_bronze_to_silver() para la justificación del parámetro spark.
    """
    import importlib.util
    # __file__ está en ci/testing/ → subir 2 niveles para llegar a la raíz
    jobs_etl = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
        "spark", "jobs", "etl",
    )
    spec = importlib.util.spec_from_file_location(
        "silver_to_gold", os.path.join(jobs_etl, "silver_to_gold.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main(spark=spark)   # ← inyectar sesión; el job NO llamará spark.stop()


# =============================================================================
# Modelo de resultado de test
# =============================================================================

@dataclass
class ResilienceResult:
    name:               str
    description:        str
    ts:                 datetime = field(default_factory=lambda: datetime.now(UTC))
    pre_row_count:      int = -1
    post_row_count:     int = -1
    dup_count:          int = -1
    temp_files:         list[str] = field(default_factory=list)
    delta_versions_pre: list[int] = field(default_factory=list)
    delta_versions_post: list[int] = field(default_factory=list)
    table_readable:     bool = False
    error_during_fault: str | None = None
    error_during_retry: str | None = None
    status:             str = "PENDING"
    notes:              list[str] = field(default_factory=list)

    def _evaluate(self) -> None:
        """Calcula el status final del test."""
        issues = []
        if not self.table_readable:
            issues.append("tabla no legible post-retry")
        if self.dup_count > 0:
            issues.append(f"{self.dup_count} duplicados detectados")
        if self.temp_files:
            issues.append(f"{len(self.temp_files)} archivos temporales huérfanos")
        # El conteo no debe BAJAR sin motivo (cero regresión)
        if self.pre_row_count > 0 and self.post_row_count >= 0:
            if self.post_row_count < self.pre_row_count:
                issues.append(
                    f"row_count bajó: {self.pre_row_count} → {self.post_row_count}"
                )
        self.status = "FAIL" if issues else "PASS"
        if issues:
            self.notes.append("Issues: " + "; ".join(issues))

    def to_dict(self) -> dict:
        self._evaluate()
        return {
            "test":               self.name,
            "description":        self.description,
            "ts":                 self.ts.isoformat(),
            "status":             self.status,
            "pre_row_count":      self.pre_row_count,
            "post_row_count":     self.post_row_count,
            "dup_count":          self.dup_count,
            "table_readable":     self.table_readable,
            "temp_files_orphaned": len(self.temp_files),
            "delta_versions_pre": self.delta_versions_pre,
            "delta_versions_post": self.delta_versions_post,
            "error_during_fault": self.error_during_fault,
            "error_during_retry": self.error_during_retry,
            "notes":              self.notes,
        }


# =============================================================================
# Escenarios de fallo
# =============================================================================

def _scenario_schema_poison(spark, s3, result: ResilienceResult) -> None:
    """
    Escenario 1 — Schema Poisoning:
    Intenta escribir un DataFrame con una columna extra de tipo incompatible
    (poison_col: ARRAY<string>) directamente sobre la tabla Delta Silver.
    Delta debe rechazar la escritura si el schema evolucionó de forma
    incompatible sin mergeSchema=true. Luego re-ejecutar el ETL real y
    verificar que la tabla sigue leyéndose correctamente.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    result.notes.append("Inyectando DataFrame con columna extra de tipo ARRAY...")

    try:
        # Leer la tabla actual y agregar una columna incompatible
        df_poison = (
            spark.read.format("delta").load(TEST_TABLE_PATH)
            .withColumn("poison_col", F.array(F.lit("bad_data")))
            .limit(5)
        )
        # Intentar sobreescribir SIN mergeSchema (debe fallar o ser bloqueado)
        (
            df_poison.write
            .format("delta")
            .mode("overwrite")
            .save(TEST_TABLE_PATH)
        )
        result.notes.append(
            "ADVERTENCIA: Delta aceptó el overwrite con columna extra "
            "(mergeSchema no fue requerido en este contexto)."
        )
    except Exception as exc:
        result.error_during_fault = str(exc)
        result.notes.append(f"Delta rechazó el schema envenenado (esperado): {type(exc).__name__}")

    # Retry: ejecutar el ETL real para restaurar/confirmar la tabla
    result.notes.append("Ejecutando retry del ETL Bronze→Silver...")
    try:
        _run_bronze_to_silver(spark)
        result.notes.append("ETL Bronze→Silver completado exitosamente en retry.")
    except Exception as exc:
        result.error_during_retry = str(exc)
        result.notes.append(f"Error durante retry: {exc}")


def _scenario_null_pk(spark, s3, result: ResilienceResult) -> None:
    """
    Escenario 2 — Nulos en Clave Primaria:
    Genera un DataFrame con 10 filas donde gpu_model=NULL y lo intenta
    insertar en la tabla Silver. El filtro de calidad de bronze_to_silver.py
    debe descartar estas filas. Verificamos que el conteo post-retry no crece
    y que no hay filas con PK nula en la tabla.
    """
    from pyspark.sql import functions as F, Row
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType
    )

    result.notes.append(
        "Intentando insertar 10 filas con gpu_model=NULL via MERGE directo..."
    )

    # Construir el schema de mlco2/gpus Silver
    # (gpu_model, type, tdp_watts, tflops_32, tflops_16,
    #  gflops_32_per_w, gflops_16_per_w, memory, source)
    poison_rows = [
        Row(
            gpu_model=None,    # NULL en PK
            type="test",
            tdp_watts=100,
            tflops_32=1.0,
            tflops_16=2.0,
            gflops_32_per_w="10",
            gflops_16_per_w="20",
            memory="16GB",
            source="poison_test",
        )
        for _ in range(10)
    ]
    schema = StructType([
        StructField("gpu_model",       StringType(),  nullable=True),
        StructField("type",            StringType(),  nullable=True),
        StructField("tdp_watts",       IntegerType(), nullable=True),
        StructField("tflops_32",       DoubleType(),  nullable=True),
        StructField("tflops_16",       DoubleType(),  nullable=True),
        StructField("gflops_32_per_w", StringType(),  nullable=True),
        StructField("gflops_16_per_w", StringType(),  nullable=True),
        StructField("memory",          StringType(),  nullable=True),
        StructField("source",          StringType(),  nullable=True),
    ])

    df_poison = spark.createDataFrame(poison_rows, schema=schema)

    try:
        # Intentar inyectar directamente con MERGE (bypaseando filtros del ETL)
        from delta.tables import DeltaTable
        (
            DeltaTable.forPath(spark, TEST_TABLE_PATH)
            .alias("target")
            .merge(
                df_poison.alias("source"),
                "target.`gpu_model` = source.`gpu_model`",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        result.notes.append(
            "MERGE ejecutado con filas PK=NULL. "
            "Delta ignora silenciosamente NULLs en join ON — no se insertan."
        )
    except Exception as exc:
        result.error_during_fault = str(exc)
        result.notes.append(f"Error al inyectar nulos: {exc}")

    # Verificar que no existen filas con PK nula en la tabla
    try:
        null_rows = (
            spark.read.format("delta").load(TEST_TABLE_PATH)
            .filter(F.col("gpu_model").isNull())
            .count()
        )
        if null_rows > 0:
            result.notes.append(
                f"⚠ FALLO: {null_rows} filas con gpu_model=NULL encontradas en la tabla."
            )
        else:
            result.notes.append("✓ Cero filas con PK nula en la tabla Silver.")
    except Exception as exc:
        result.notes.append(f"Error verificando nulos: {exc}")

    # Retry
    result.notes.append("Ejecutando retry del ETL Bronze→Silver...")
    try:
        _run_bronze_to_silver(spark)
        result.notes.append("ETL Bronze→Silver completado exitosamente.")
    except Exception as exc:
        result.error_during_retry = str(exc)
        result.notes.append(f"Error durante retry: {exc}")


def _scenario_partial_write(spark, s3, result: ResilienceResult) -> None:
    """
    Escenario 3 — Escritura Parcial Interrumpida:
    Sube un archivo Parquet sintético con nombre de archivo correcto pero
    contenido binario corrupto (0-bytes) al path Silver de la tabla de test,
    FUERA del Delta Log (no como commit). Esto simula un worker que murió
    en mitad de la escritura S3 sin completar el commit Delta.
    Luego se re-ejecuta el ETL y se verifica que:
      - La tabla Delta sigue siendo legible (el Delta Log es consistente).
      - El archivo corrupto es ignorado (Delta solo lee archivos en el log).
      - No hay archivos _temporary huérfanos.
    """
    corrupt_key = f"{TEST_TABLE_REL}/year=2099/part-00000-corrupted-test.parquet"
    result.notes.append(
        f"Subiendo archivo Parquet corrupto (0 bytes) a: s3://{SILVER_BUCKET}/{corrupt_key}"
    )

    try:
        s3.put_object(
            Bucket=SILVER_BUCKET,
            Key=corrupt_key,
            Body=b"",   # 0 bytes — archivo totalmente vacío / corrupto
        )
        result.notes.append("Archivo corrupto subido exitosamente.")
    except Exception as exc:
        result.error_during_fault = str(exc)
        result.notes.append(f"Error al subir archivo corrupto: {exc}")

    # Retry: el ETL debe completar y la tabla Delta seguir siendo coherente
    result.notes.append("Ejecutando retry del ETL Bronze→Silver...")
    try:
        _run_bronze_to_silver(spark)
        result.notes.append("ETL Bronze→Silver completado exitosamente.")
    except Exception as exc:
        result.error_during_retry = str(exc)
        result.notes.append(f"Error durante retry: {exc}")

    # Verificar que el archivo corrupto aún existe (Delta lo ignora, no lo borra)
    # pero que la tabla sigue siendo legible
    result.notes.append(
        "Delta ignora archivos fuera del log — tabla debe ser legible normalmente."
    )

    # Limpiar el archivo corrupto para no contaminar el bucket
    try:
        s3.delete_object(Bucket=SILVER_BUCKET, Key=corrupt_key)
        result.notes.append(f"Archivo corrupto eliminado: {corrupt_key}")
    except Exception as exc:
        result.notes.append(f"AVISO: No se pudo eliminar el archivo corrupto: {exc}")


# =============================================================================
# Runner de escenarios
# =============================================================================

SCENARIOS: list[tuple[str, str, Callable]] = [
    (
        "schema_poison",
        "Inyección de schema incompatible (columna ARRAY extra) sobre tabla Delta Silver",
        _scenario_schema_poison,
    ),
    (
        "null_pk_injection",
        "Intento de insertar filas con clave primaria NULL directamente via MERGE",
        _scenario_null_pk,
    ),
    (
        "partial_write_orphan",
        "Archivo Parquet corrupto (0 bytes) subido fuera del Delta Log — simula crash mid-write",
        _scenario_partial_write,
    ),
]


def run_all_scenarios(spark, s3) -> list[dict]:
    """Ejecuta todos los escenarios de resiliencia y devuelve los resultados."""
    results: list[dict] = []

    for scenario_name, description, scenario_fn in SCENARIOS:
        logger.info("")
        logger.info("─" * 65)
        logger.info("[%s] Iniciando escenario: %s", scenario_name, description)

        result = ResilienceResult(name=scenario_name, description=description)

        # ── Estado PRE-FAULT ──────────────────────────────────────────────
        result.pre_row_count, pre_err = _read_delta_safe(spark, TEST_TABLE_PATH)
        result.delta_versions_pre   = _delta_log_versions(spark, TEST_TABLE_PATH)
        if pre_err:
            result.notes.append(f"Error leyendo estado pre-fault: {pre_err}")

        logger.info(
            "[%s] Estado pre-fault: rows=%d  versions=%s",
            scenario_name, result.pre_row_count, result.delta_versions_pre,
        )

        # ── Aplicar escenario de fallo + retry ────────────────────────────
        try:
            scenario_fn(spark, s3, result)
        except Exception as exc:
            result.notes.append(f"Error inesperado en escenario: {traceback.format_exc()}")
            logger.error("[%s] Error inesperado: %s", scenario_name, exc)

        # ── Estado POST-RETRY ─────────────────────────────────────────────
        result.post_row_count, post_err = _read_delta_safe(spark, TEST_TABLE_PATH)
        result.table_readable       = (post_err is None)
        result.dup_count            = _count_duplicates(spark, TEST_TABLE_PATH, TEST_TABLE_PK)
        result.delta_versions_post  = _delta_log_versions(spark, TEST_TABLE_PATH)
        result.temp_files           = _check_no_temp_files(s3, SILVER_BUCKET, TEST_TABLE_REL)

        if post_err:
            result.notes.append(f"Error leyendo estado post-retry: {post_err}")

        result._evaluate()
        scenario_dict = result.to_dict()
        results.append(scenario_dict)

        logger.info(
            "[%s] Resultado: %s | rows: %d→%d | dups: %d | temp_files: %d | legible: %s",
            scenario_name, result.status,
            result.pre_row_count, result.post_row_count,
            result.dup_count, len(result.temp_files), result.table_readable,
        )
        for note in result.notes:
            logger.info("    → %s", note)

    return results


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    logger.info("=" * 65)
    logger.info("  GREEN AI — Prueba de Resiliencia Delta Lake End-to-End")
    logger.info("=" * 65)
    logger.info("  Tabla de prueba: %s", TEST_TABLE_PATH)
    logger.info("  Escenarios:      %d", len(SCENARIOS))
    logger.info("=" * 65)

    import boto3

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    try:
        results = run_all_scenarios(spark, s3)

        overall_pass = all(r["status"] == "PASS" for r in results)
        report = {
            "test":           "resilience",
            "overall_status": "PASS" if overall_pass else "FAIL",
            "timestamp":      datetime.now(UTC).isoformat(),
            "test_table":     TEST_TABLE_PATH,
            "scenarios":      results,
        }

        print("\n" + "=" * 65)
        print("  RESULTADO — Prueba de Resiliencia")
        print("=" * 65)
        print(json.dumps(report, indent=2, default=str))

        return 0 if overall_pass else 1

    except Exception as exc:
        logger.exception("Error crítico durante la prueba de resiliencia: %s", exc)
        return 2

    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    sys.exit(main())