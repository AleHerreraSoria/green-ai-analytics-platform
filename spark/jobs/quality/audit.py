"""
audit.py — Green AI Analytics Platform
=======================================
Módulo de observabilidad Bronze → Silver.

Responsabilidades:
  - Validar la existencia de paths antes de leer.
  - Contar registros en Bronze (CSV / JSON) y Silver (Delta).
  - Calcular métricas sólo cuando ambos conteos son válidos.
  - Clasificar el resultado con un estado explícito y no ambiguo.
  - Persistir el log de auditoría en Delta (Silver) de forma idempotente.
  - Emitir un reporte legible en consola compatible con Airflow / Step Functions.

Estados posibles
----------------
  OK                    — Retención dentro del umbral aceptable.
  INFO_EXPANSION        — Silver tiene más filas que Bronze (esperado por explode).
  WARN_DATA_LOSS        — Pérdida de datos superior al umbral configurado.
  WARN_EMPTY_BRONZE     — Bronze existe pero no contiene registros.
  ERROR_PATH            — Path Bronze o Silver no encontrado en S3.
  ERROR_TABLE_NOT_FOUND — Tabla Delta Silver no inicializada.
  ERROR_READ_FAILURE    — Fallo inesperado durante la lectura.

Configuración
-------------
  S3_BRONZE_BUCKET / S3_SILVER_BUCKET  (env vars, con defaults).
  MAX_LOSS_THRESHOLD     — % máximo de pérdida aceptable (default 5 %).
  EXPANSION_THRESHOLD    — % mínimo de crecimiento para marcar INFO_EXPANSION.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone
UTC = timezone.utc
from typing import Optional

# ── dotenv (opcional, sólo en desarrollo local) ────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.audit")


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


# ===========================================================================
# 1. CONFIGURACIÓN CENTRALIZADA
# ===========================================================================

BRONZE_BUCKET: str = require_env("S3_BRONZE_BUCKET")
SILVER_BUCKET: str = require_env("S3_SILVER_BUCKET")
BRONZE: str = f"s3a://{BRONZE_BUCKET}"
SILVER: str = f"s3a://{SILVER_BUCKET}"

# Umbrales
MAX_LOSS_THRESHOLD: float = 5.0    # % de pérdida máxima aceptable
EXPANSION_THRESHOLD: float = 150.0 # % mínimo para clasificar como INFO_EXPANSION

# Catálogo de datasets — única fuente de verdad
# Cada entrada define dónde leer Bronze, dónde leer Silver y cómo leerlos.
#
# Campos opcionales:
#   "skip_rows"         — filas de cabecera extra en CSVs World Bank
#   "explode_col"       — columna de array a expandir en JSONs
#   "bronze_counter"    — clave especial para lógica de conteo personalizada
DATASETS: dict[str, dict] = {
    "usage_logs": {
        "bronze_path":  f"{BRONZE}/usage_logs/usage_logs.csv",
        "silver_path":  f"{SILVER}/usage_logs",
        "bronze_format": "csv",
    },
    "global_petrol_prices": {
        "bronze_path":  f"{BRONZE}/global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv",
        "silver_path":  f"{SILVER}/global_petrol_prices",
        "bronze_format": "csv",
    },
    "mlco2/impact": {
        "bronze_path":  f"{BRONZE}/mlco2/impact.csv",
        "silver_path":  f"{SILVER}/mlco2/impact",
        "bronze_format": "csv",
    },
    "mlco2/instances": {
        "bronze_path":  f"{BRONZE}/mlco2/instances.csv",
        "silver_path":  f"{SILVER}/mlco2/instances",
        "bronze_format": "csv",
    },
    "mlco2/gpus": {
        "bronze_path":  f"{BRONZE}/mlco2/gpus.csv",
        "silver_path":  f"{SILVER}/mlco2/gpus",
        "bronze_format": "csv",
    },
    "world_bank/metadata": {
        "bronze_path":  f"{BRONZE}/world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
        "silver_path":  f"{SILVER}/reference/world_bank_metadata",
        "bronze_format": "csv",
        "skip_rows": 4,
    },
    "world_bank/ict_exports": {
        "bronze_path":  f"{BRONZE}/world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
        "silver_path":  f"{SILVER}/world_bank/ict_exports",
        "bronze_format": "csv",
        "skip_rows": 4,
    },
    "owid": {
        "bronze_path":  f"{BRONZE}/owid/owid-energy-data.csv",
        "silver_path":  f"{SILVER}/owid",
        "bronze_format": "csv",
    },
    # ── electricity_maps — un dataset por endpoint (alineado con Silver) ──
    "electricity_maps/carbon_intensity/latest": {
        # Bronze: JSONs planos por zona (sin array)
        # Silver: escrito por process_carbon_intensity_flat(spark, 'latest')
        "bronze_path":  f"{BRONZE}/electricity_maps/carbon_intensity/latest",
        "silver_path":  f"{SILVER}/electricity_maps/carbon_intensity/latest",
        "bronze_format": "json_recursive",
    },
    "electricity_maps/carbon_intensity/past": {
        # Bronze: JSONs planos por zona + datetime (sin array)
        # Silver: escrito por process_carbon_intensity_flat(spark, 'past')
        "bronze_path":  f"{BRONZE}/electricity_maps/carbon_intensity/past",
        "silver_path":  f"{SILVER}/electricity_maps/carbon_intensity/past",
        "bronze_format": "json_recursive",
    },
    "electricity_maps/carbon_intensity/history": {
        # Bronze: JSONs con array 'history[]' → explode genera 1 fila por evento
        # Silver: escrito por process_carbon_intensity_history(spark)
        "bronze_path":  f"{BRONZE}/electricity_maps/carbon_intensity/history",
        "silver_path":  f"{SILVER}/electricity_maps/carbon_intensity/history",
        "bronze_format": "json_recursive_explode",
        "explode_col":  "history",   # columna de array a expandir
    },
    "electricity_maps/electricity_mix/latest": {
        # Bronze: JSONs con array 'powerConsumptionBreakdown[]' → explode
        # Silver: escrito por process_electricity_mix(spark)
        "bronze_path":  f"{BRONZE}/electricity_maps/electricity_mix/latest",
        "silver_path":  f"{SILVER}/electricity_maps/electricity_mix/latest",
        "bronze_format": "json_recursive_explode",
        "explode_col":  "powerConsumptionBreakdown",
    },
}


# ===========================================================================
# 2. MODELO DE DATOS — AuditResult
# ===========================================================================

class AuditResult:
    """Resultado inmutable de la auditoría de un dataset."""

    # Estados aceptados
    OK                    = "OK"
    INFO_EXPANSION        = "INFO_EXPANSION"
    WARN_DATA_LOSS        = "WARN_DATA_LOSS"
    WARN_EMPTY_BRONZE     = "WARN_EMPTY_BRONZE"
    ERROR_PATH            = "ERROR_PATH"
    ERROR_TABLE_NOT_FOUND = "ERROR_TABLE_NOT_FOUND"
    ERROR_READ_FAILURE    = "ERROR_READ_FAILURE"

    def __init__(
        self,
        dataset: str,
        bronze_count: Optional[int],
        silver_count: Optional[int],
        status: str,
        run_timestamp: datetime,
        error_detail: Optional[str] = None,
    ) -> None:
        self.dataset       = dataset
        self.bronze_count  = bronze_count
        self.silver_count  = silver_count
        self.status        = status
        self.run_timestamp = run_timestamp
        self.error_detail  = error_detail

    # ── Métricas derivadas — sólo cuando ambos conteos son válidos ──────────

    @property
    def delta(self) -> Optional[int]:
        if self.bronze_count is None or self.silver_count is None:
            return None
        return self.silver_count - self.bronze_count

    @property
    def retention_pct(self) -> Optional[float]:
        if self.bronze_count is None or self.silver_count is None:
            return None
        if self.bronze_count == 0:
            return None
        return round((self.silver_count / self.bronze_count) * 100, 2)

    def to_dict(self) -> dict:
        return {
            "dataset":       self.dataset,
            "bronze_count":  self.bronze_count,
            "silver_count":  self.silver_count,
            "delta":         self.delta,
            "retention_pct": self.retention_pct,
            "status":        self.status,
            "error_detail":  self.error_detail,
            "timestamp":     self.run_timestamp.isoformat(),
        }


# ===========================================================================
# 3. FUNCIONES DE INFRAESTRUCTURA — Validación de paths
# ===========================================================================

def _s3_key_exists(bucket: str, key: str) -> bool:
    """Verifica si un objeto existe en S3 sin descargarlo."""
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def _s3_prefix_has_objects(bucket: str, prefix: str) -> bool:
    """Verifica si un prefijo S3 contiene al menos un objeto."""
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/", MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Descompone 's3a://bucket/key' → (bucket, key)."""
    path = s3_path.replace("s3a://", "").replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


def _bronze_path_exists(path: str) -> bool:
    """True si el path Bronze (archivo o prefijo) existe en S3."""
    bucket, key = _parse_s3_path(path)
    # Puede ser un archivo individual o un prefijo (carpeta)
    return _s3_key_exists(bucket, key) or _s3_prefix_has_objects(bucket, key)


def _silver_table_exists(spark: SparkSession, path: str) -> bool:
    """True si la tabla Delta Silver existe y es legible."""
    try:
        spark.read.format("delta").load(path).limit(0).count()
        return True
    except Exception as e:
        err = str(e)
        if "DELTA_TABLE_NOT_FOUND" in err or "PATH_NOT_FOUND" in err:
            return False
        raise


# ===========================================================================
# 4. CONTEOS BRONZE
# ===========================================================================

def _count_bronze_csv(path: str, skip_rows: int = 0) -> int:
    """Cuenta filas de un CSV en S3 usando boto3 + pandas (sin Spark workers)."""
    bucket, key = _parse_s3_path(path)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_csv(io.BytesIO(body), skiprows=skip_rows, dtype=str)
    return len(df)


def _count_bronze_json_recursive(spark: SparkSession, path: str) -> int:
    """Cuenta registros en un directorio JSON recursivo (un objeto JSON = una fila)."""
    return spark.read.option("recursiveFileLookup", "true").json(path).count()


def _count_bronze_json_recursive_explode(
    spark: SparkSession, path: str, explode_col: str
) -> int:
    """
    Cuenta eventos individuales dentro de archivos JSON que contienen un array.

    Replica la lógica de bronze_to_silver.py (process_carbon_intensity_history
    y process_electricity_mix): lee el JSON completo y luego hace explode sobre
    la columna de array indicada, de modo que el conteo Bronze sea comparable
    con el conteo Silver que ya tiene una fila por evento.

    Args:
        spark:       SparkSession activa.
        path:        Prefijo S3 a leer (recursiveFileLookup=true).
        explode_col: Nombre del campo de array a expandir (ej. 'history').
    """
    df = spark.read.option("recursiveFileLookup", "true").option("multiLine", "true").json(path)
    if explode_col not in df.columns:
        logger.warning(
            "Columna '%s' no encontrada en %s — columnas disponibles: %s",
            explode_col, path, df.columns,
        )
        # Fallback: contar filas sin expandir
        return df.count()
    return df.withColumn("_item", F.explode(explode_col)).count()


def _count_bronze_zones_union(spark: SparkSession, base_path: str) -> int:
    """
    Cuenta zonas únicas observadas a través de todos los endpoints
    de Electricity Maps (carbon_intensity: latest, past, history).
    Este es el valor semánticamente correcto a comparar con Silver,
    donde cada zona tiene exactamente una fila.
    """
    def _read_zones(path: str, is_history: bool = False):
        reader = (
            spark.read
            .option("recursiveFileLookup", "true")
            .option("multiLine", "true")
            .json(path)
        )
        if is_history:
            return reader.withColumn("_ev", F.explode("history")).select(
                F.col("_ev.zone").alias("zone_key")
            )
        return reader.select(F.col("zone").alias("zone_key"))

    dfs = []
    for sub, history in [("latest", False), ("past", False), ("history", True)]:
        sub_path = f"{base_path}/carbon_intensity/{sub}"
        bucket, key = _parse_s3_path(sub_path)
        if _s3_prefix_has_objects(bucket, key):
            dfs.append(_read_zones(sub_path, is_history=history))

    if not dfs:
        return 0

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df)

    return combined.filter(F.col("zone_key").isNotNull()).distinct().count()


def _count_silver_delta(spark: SparkSession, path: str) -> int:
    """Cuenta filas en una tabla Delta Silver."""
    return spark.read.format("delta").load(path).count()


# ===========================================================================
# 5. LÓGICA DE CLASIFICACIÓN
# ===========================================================================

def _classify(
    bronze_count: int,
    silver_count: int,
) -> str:
    """Determina el estado de auditoría dado ambos conteos son válidos."""
    if bronze_count == 0:
        return AuditResult.WARN_EMPTY_BRONZE

    retention = (silver_count / bronze_count) * 100

    if retention > EXPANSION_THRESHOLD:
        return AuditResult.INFO_EXPANSION

    loss_pct = 100 - retention
    if loss_pct > MAX_LOSS_THRESHOLD:
        return AuditResult.WARN_DATA_LOSS

    return AuditResult.OK


# ===========================================================================
# 6. AUDITORÍA POR DATASET
# ===========================================================================

def _audit_one(
    spark: SparkSession,
    dataset: str,
    cfg: dict,
    run_ts: datetime,
) -> AuditResult:
    """Ejecuta la auditoría de un único dataset y devuelve un AuditResult."""
    bronze_path  = cfg["bronze_path"]
    silver_path  = cfg["silver_path"]
    bronze_fmt   = cfg.get("bronze_format", "csv")
    skip_rows    = cfg.get("skip_rows", 0)

    # ── Validación previa Bronze ───────────────────────────────────────────
    try:
        bronze_ok = _bronze_path_exists(bronze_path)
    except Exception as e:
        logger.warning("[%s] No se pudo verificar Bronze: %s", dataset, e)
        bronze_ok = False

    if not bronze_ok:
        return AuditResult(
            dataset=dataset,
            bronze_count=None,
            silver_count=None,
            status=AuditResult.ERROR_PATH,
            run_timestamp=run_ts,
            error_detail=f"Bronze path no encontrado: {bronze_path}",
        )

    # ── Validación previa Silver ───────────────────────────────────────────
    try:
        silver_ok = _silver_table_exists(spark, silver_path)
    except Exception as e:
        logger.warning("[%s] Error inesperado validando Silver: %s", dataset, e)
        silver_ok = False

    if not silver_ok:
        return AuditResult(
            dataset=dataset,
            bronze_count=None,
            silver_count=None,
            status=AuditResult.ERROR_TABLE_NOT_FOUND,
            run_timestamp=run_ts,
            error_detail=f"Tabla Delta Silver no encontrada: {silver_path}",
        )

    # ── Conteo Bronze ─────────────────────────────────────────────────────
    try:
        if bronze_fmt == "csv":
            bronze_count = _count_bronze_csv(bronze_path, skip_rows=skip_rows)
        elif bronze_fmt == "json_recursive":
            bronze_count = _count_bronze_json_recursive(spark, bronze_path)
        elif bronze_fmt == "json_recursive_explode":
            explode_col = cfg.get("explode_col", "")
            if not explode_col:
                raise ValueError(f"'explode_col' requerido para bronze_format=json_recursive_explode en {dataset}")
            bronze_count = _count_bronze_json_recursive_explode(spark, bronze_path, explode_col)
        elif bronze_fmt == "zones_union":
            # Deprecated: reemplazado por entradas específicas por endpoint
            bronze_count = _count_bronze_zones_union(spark, bronze_path)
        else:
            raise ValueError(f"bronze_format desconocido: {bronze_fmt}")
    except Exception as e:
        logger.error("[%s] Error al contar Bronze: %s", dataset, e)
        return AuditResult(
            dataset=dataset,
            bronze_count=None,
            silver_count=None,
            status=AuditResult.ERROR_READ_FAILURE,
            run_timestamp=run_ts,
            error_detail=f"Error leyendo Bronze: {e}",
        )

    # ── Conteo Silver ─────────────────────────────────────────────────────
    try:
        silver_count = _count_silver_delta(spark, silver_path)
    except Exception as e:
        logger.error("[%s] Error al contar Silver: %s", dataset, e)
        return AuditResult(
            dataset=dataset,
            bronze_count=bronze_count,
            silver_count=None,
            status=AuditResult.ERROR_READ_FAILURE,
            run_timestamp=run_ts,
            error_detail=f"Error leyendo Silver: {e}",
        )

    # ── Clasificación ─────────────────────────────────────────────────────
    status = _classify(bronze_count, silver_count)
    return AuditResult(
        dataset=dataset,
        bronze_count=bronze_count,
        silver_count=silver_count,
        status=status,
        run_timestamp=run_ts,
    )


# ===========================================================================
# 7. LOOP PRINCIPAL
# ===========================================================================

def run_audit(spark: SparkSession) -> list[AuditResult]:
    """Itera sobre todos los datasets y devuelve la lista de AuditResults."""
    run_ts = datetime.now(UTC)
    results: list[AuditResult] = []

    for dataset, cfg in DATASETS.items():
        logger.info("Auditando: %s", dataset)
        result = _audit_one(spark, dataset, cfg, run_ts)
        results.append(result)
        _print_result(result)

    return results


def _print_result(r: AuditResult) -> None:
    """Imprime el resultado de un dataset en formato legible."""
    icon = {
        AuditResult.OK:                    "✅",
        AuditResult.INFO_EXPANSION:        "📈",
        AuditResult.WARN_DATA_LOSS:        "⚠️ ",
        AuditResult.WARN_EMPTY_BRONZE:     "⚠️ ",
        AuditResult.ERROR_PATH:            "❌",
        AuditResult.ERROR_TABLE_NOT_FOUND: "❌",
        AuditResult.ERROR_READ_FAILURE:    "❌",
    }.get(r.status, "❓")

    print(f"\n  {icon} [{r.status}] {r.dataset}")

    if r.bronze_count is not None:
        print(f"       Bronze:     {r.bronze_count:>12,}")
    if r.silver_count is not None:
        print(f"       Silver:     {r.silver_count:>12,}")
    if r.delta is not None:
        print(f"       Delta:      {r.delta:>+12,}")
    if r.retention_pct is not None:
        print(f"       Retención:  {r.retention_pct:>10.1f}%")
    if r.error_detail:
        print(f"       Detalle:    {r.error_detail}")


# ===========================================================================
# 8. SCHEMA DEL LOG Y PERSISTENCIA
# ===========================================================================

AUDIT_LOG_SCHEMA = StructType([
    StructField("dataset",       StringType(),    nullable=False),
    StructField("bronze_count",  IntegerType(),   nullable=True),
    StructField("silver_count",  IntegerType(),   nullable=True),
    StructField("delta",         IntegerType(),   nullable=True),
    StructField("retention_pct", DoubleType(),    nullable=True),
    StructField("status",        StringType(),    nullable=False),
    StructField("error_detail",  StringType(),    nullable=True),
    StructField("run_timestamp", TimestampType(), nullable=False),
])


def persist_audit_log(spark: SparkSession, results: list[AuditResult]) -> None:
    """
    Persiste el log de auditoría en Delta (Silver) de forma idempotente.
    Usa PyArrow (pandas bridge) para evitar el bug de cloudpickle en Python 3.12+.
    La escritura usa replaceWhere para sobrescribir sólo la partición del día.
    """
    run_date_str = results[0].run_timestamp.strftime("%Y-%m-%d") if results else \
                   datetime.now(UTC).strftime("%Y-%m-%d")

    rows = [
        {
            "dataset":       r.dataset,
            "bronze_count":  r.bronze_count,
            "silver_count":  r.silver_count,
            "delta":         r.delta,
            "retention_pct": r.retention_pct,
            "status":        r.status,
            "error_detail":  r.error_detail,
            "run_timestamp": r.run_timestamp,
        }
        for r in results
    ]

    pdf = pd.DataFrame(rows)
    df_log = (
        spark.createDataFrame(pdf, schema=AUDIT_LOG_SCHEMA)
        .withColumn("run_date", F.to_date("run_timestamp"))
    )

    log_path = f"{SILVER}/audit/audit_log"

    def _do_write(overwrite_schema: bool = False) -> None:
        writer = (
            df_log.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("run_date")
        )
        if overwrite_schema:
            # Disable dynamic partition overwrite temporarily via SparkContext conf
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("replaceWhere", f"run_date = '{run_date_str}'")
        writer.save(log_path)
        if overwrite_schema:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        _do_write(overwrite_schema=False)
    except Exception as e:
        if "schema mismatch" in str(e).lower() or "_LEGACY_ERROR_TEMP_DELTA_0007" in str(e):
            logger.warning("Schema mismatch detectado — migrando schema de audit_log...")
            _do_write(overwrite_schema=True)
        else:
            raise
    logger.info("Log de auditoría persistido en: %s (run_date=%s)", log_path, run_date_str)


# ===========================================================================
# 9. RESUMEN FINAL
# ===========================================================================

def _print_summary(results: list[AuditResult]) -> None:
    ok_statuses   = {AuditResult.OK, AuditResult.INFO_EXPANSION}
    warn_statuses = {AuditResult.WARN_DATA_LOSS, AuditResult.WARN_EMPTY_BRONZE}
    err_statuses  = {AuditResult.ERROR_PATH, AuditResult.ERROR_TABLE_NOT_FOUND,
                     AuditResult.ERROR_READ_FAILURE}

    oks    = [r for r in results if r.status in ok_statuses]
    warns  = [r for r in results if r.status in warn_statuses]
    errors = [r for r in results if r.status in err_statuses]

    print("\n" + "=" * 64)
    print("  RESUMEN AUDITORÍA Bronze → Silver — Green AI")
    print("=" * 64)
    print(f"  Total datasets auditados : {len(results)}")
    print(f"  ✅  OK / Expansion        : {len(oks)}")
    print(f"  ⚠️   Warnings              : {len(warns)}")
    print(f"  ❌  Errores de infra      : {len(errors)}")

    if warns:
        print("\n  Datasets con pérdida de datos:")
        for r in warns:
            print(f"    - {r.dataset}: {r.status}  (retención={r.retention_pct}%)")

    if errors:
        print("\n  Datasets con errores de infraestructura:")
        for r in errors:
            print(f"    - {r.dataset}: {r.status}")
            print(f"        {r.error_detail}")

    print("=" * 64 + "\n")

    # Salida estructurada para consumo por sistemas de observabilidad
    print("AUDIT_JSON_OUTPUT:", json.dumps([r.to_dict() for r in results], default=str))


# ===========================================================================
# 10. SPARK SESSION
# ===========================================================================

def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("green-ai-audit-bronze-silver")
        .config("spark.sql.session.timeZone", "UTC")
        # PyArrow: evita bug de cloudpickle en Python 3.12+ con createDataFrame
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Dependencias
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "software.amazon.awssdk:bundle:2.20.18")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Fix Hadoop 3.3.4 — timeouts como milisegundos (no sufijos "s")
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


# ===========================================================================
# 11. ENTRYPOINT
# ===========================================================================

def main() -> None:
    print("\n" + "=" * 64)
    print("  AUDITORÍA Bronze → Silver — Green AI Analytics Platform")
    print("=" * 64)

    spark = _build_spark()
    try:
        results = run_audit(spark)
        persist_audit_log(spark, results)
        _print_summary(results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
