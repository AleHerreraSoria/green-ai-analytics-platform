"""
bronze_to_silver.py
====================
Job principal de transformación Bronze → Silver para el proyecto Green AI.

Ejecutar en producción (EC2/EMR) con spark-submit:
    spark-submit --master yarn \\
        --deploy-mode cluster \\
        --py-files utils.zip \\
        bronze_to_silver.py

Variables de entorno requeridas:
    S3_BRONZE_BUCKET, S3_SILVER_BUCKET
    En producción son inyectadas por el IAM Role / Airflow.
    En desarrollo local se pueden definir en un archivo .env.
"""

from __future__ import annotations
import logging
import os
import sys

# ── Logging ────────────────────────────────────────────────────────────────
# En EC2/EMR los logs se capturan por CloudWatch Agent o el log driver de
# YARN. Usar logging en lugar de print() permite configurar niveles y
# formatos sin tocar el código (ej. desde un archivo log4j2.properties).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.bronze_to_silver")

# ── Dependencias opcionales de desarrollo ──────────────────────────────────
# En producción (EC2/Airflow) no existirá el archivo .env físico.
# Las variables S3_BRONZE_BUCKET y S3_SILVER_BUCKET serán inyectadas
# directamente en el entorno del sistema operativo por Airflow o el
# IAM Role de la instancia. Si dotenv no está instalado o no hay .env,
# el código continúa silenciosamente.
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.debug(".env cargado (entorno de desarrollo local).")
except ImportError:
    logger.debug("python-dotenv no instalado — se asume entorno de producción.")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Librerías compartidas de Spark (spark/libs).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from libs.schemas import (
    CARBON_INTENSITY_FLAT_SCHEMA,
    CARBON_INTENSITY_HISTORY_RAW_SCHEMA,
    EC2_PRICING_SCHEMA,
    ELECTRICITY_MIX_RAW_SCHEMA,
    MLCO2_YEARLY_AVG_SCHEMA,
    USAGE_LOGS_SCHEMA,
    MLCO2_INSTANCES_SCHEMA,
    MLCO2_IMPACT_SCHEMA,
    MLCO2_GPUS_SCHEMA,
    WORLD_BANK_ICT_SCHEMA,
    GEO_CLOUD_MAPPING_SCHEMA,
    GLOBAL_PETROL_PRICES_SCHEMA,
    OWID_ENERGY_SCHEMA,
    WORLD_BANK_METADATA_SCHEMA,
)
from libs.writer import WriteResult, write_to_silver

BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "green-ai-pf-bronze-a0e96d06")
SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")

BRONZE = f"s3a://{BRONZE_BUCKET}"
SILVER = f"s3a://{SILVER_BUCKET}"

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("green-ai-bronze-to-silver")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        # Particiones HIVE dinámicas
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # ── SELF-HEALING: desactivar modo ANSI ──────────────────────────────
        # Spark 4 activa ANSI por defecto. Con ANSI activo, un cast de
        # timestamp inválido (ej. "N/A" o "") lanza ansiDateTimeParseError
        # y aborta el Job completo. Con ANSI desactivado, el cast retorna
        # null silenciosamente y la fila se filtra en el paso siguiente.
        .config("spark.sql.ansi.enabled", "false")
        # ── Conectividad S3 (s3a://) ─────────────────────────────────────────
        # hadoop-aws descarga el conector S3A y aws-java-sdk-bundle provee
        # el cliente AWS. Las credenciales se leen del entorno (cargado por
        # dotenv / IAM Role) mediante EnvironmentVariableCredentialsProvider.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "software.amazon.awssdk:bundle:2.20.18")
        # By-pass the credential provider chain, providing keys explicitly
        # eliminates 4-minute EC2 metadata timeouts on AWS Local Run.
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

# ---------------------------------------------------------------------------
# Helpers de calidad (Self-Healing)
# ---------------------------------------------------------------------------
# Nota: la escritura se delega COMPLETAMENTE a write_to_silver() de writer.py.
# Las estrategias de particionado y la compresión Snappy están centralizadas allí.

def _safe_timestamp(col_name: str, fmt: str) -> F.Column:
    """
    Safe cast de string a Timestamp garantizado para Spark 4.x.
    Usa try_to_timestamp() que devuelve NULL para strings inválidos
    sin lanzar excepción, incluso con spark.sql.ansi.enabled=true.
    El llamador filtra los nulos resultantes en columnas clave.
    """
    return F.try_to_timestamp(F.col(col_name), F.lit(fmt)).alias(col_name)


def _log_dropped(df_before, df_after, label: str):
    """Registra cuántas filas fueron descartadas por filtros de calidad."""
    n_before = df_before.count()
    n_after  = df_after.count()
    dropped  = n_before - n_after
    if dropped > 0:
        pct = dropped / n_before * 100 if n_before else 0
        logger.warning(
            "[QA:%s] %d filas descartadas (%.1f%% de %d) → %d enviadas a Silver.",
            label, dropped, pct, n_before, n_after
        )
    else:
        logger.info("[QA:%s] Sin filas descartadas (%d total).", label, n_before)
    return df_after


# ===========================================================================
# TRANSFORMACIONES POR DATASET
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. Carbon Intensity — Latest & Past (estructura plana)
# ---------------------------------------------------------------------------

def process_carbon_intensity_flat(spark: SparkSession, endpoint: str) -> tuple[DataFrame, int]:
    """
    Lee los JSON de Latest y Past que ya tienen estructura plana.
    Usa las particiones Hive (zone=X) y las particiones de fecha del path Past.
    """
    bronze_path = f"{BRONZE}/electricity_maps/carbon_intensity/{endpoint}"

    df_raw = (
        spark.read
        .schema(CARBON_INTENSITY_FLAT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )

    df_renamed = (
        df_raw
        # camelCase → snake_case
        .withColumnRenamed("carbonIntensity",     "carbon_intensity")
        .withColumnRenamed("updatedAt",            "updated_at")
        .withColumnRenamed("createdAt",            "created_at")
        .withColumnRenamed("emissionFactorType",   "emission_factor_type")
        .withColumnRenamed("isEstimated",          "is_estimated")
        .withColumnRenamed("estimationMethod",     "estimation_method")
        .withColumnRenamed("temporalGranularity",  "temporal_granularity")
        .drop("_disclaimer")
        .withColumn("year",  F.year("datetime"))
        .withColumn("month", F.month("datetime"))
    )

    # ── SELF-HEALING: Filtros de dominio ────────────────────────────────────
    # CA Jira: carbonIntensity debe estar en [0, 1000] gCO₂eq/kWh.
    # Filas con valores sintéticos corruptos (negativos o > 1000) se descartan.
    df_clean = df_renamed.filter(
        F.col("carbon_intensity").isNotNull() &
        (F.col("carbon_intensity") >= 0) &
        (F.col("carbon_intensity") <= 1000) &
        F.col("datetime").isNotNull() &
        F.col("zone").isNotNull()
    )
    df = _log_dropped(df_renamed, df_clean, f"carbon_intensity/{endpoint}")

    dataset_key = f"electricity_maps/carbon_intensity/{endpoint}"
    result: WriteResult = write_to_silver(df, dataset_key)
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 2. Carbon Intensity — History (array → explode)
# ---------------------------------------------------------------------------

def process_carbon_intensity_history(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/electricity_maps/carbon_intensity/history"

    df_raw = (
        spark.read
        .schema(CARBON_INTENSITY_HISTORY_RAW_SCHEMA)
        # multiLine=true: el JSON de history es un objeto multi-línea con indent=2
        # (array 'history' se extiende por múltiples líneas). Sin esta opción,
        # Spark lee cada línea como un documento separado y genera _corrupt_record.
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )

    df_raw_exp = (
        df_raw
        .withColumn("event", F.explode("history"))
        .select(
            F.col("event.zone").alias("zone"),
            F.col("event.carbonIntensity").alias("carbon_intensity"),
            F.try_to_timestamp(
                F.col("event.datetime"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).alias("datetime"),
            F.try_to_timestamp(
                F.col("event.updatedAt"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).alias("updated_at"),
            F.try_to_timestamp(
                F.col("event.createdAt"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).alias("created_at"),
            F.col("event.emissionFactorType").alias("emission_factor_type"),
            F.col("event.isEstimated").alias("is_estimated"),
            F.col("event.estimationMethod").alias("estimation_method"),
            F.col("event.temporalGranularity").alias("temporal_granularity"),
        )
        .withColumn("year",  F.year("datetime"))
        .withColumn("month", F.month("datetime"))
    )

    # ── SELF-HEALING: Filtros de dominio history ──────────────────────────────
    df_clean = df_raw_exp.filter(
        F.col("carbon_intensity").isNotNull() &
        (F.col("carbon_intensity") >= 0) &
        (F.col("carbon_intensity") <= 1000) &
        F.col("datetime").isNotNull() &
        F.col("zone").isNotNull()
    )
    df = _log_dropped(df_raw_exp, df_clean, "carbon_intensity/history")

    result: WriteResult = write_to_silver(df, "electricity_maps/carbon_intensity/history")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 3. Electricity Mix — Latest (array data → explode)
# ---------------------------------------------------------------------------

def process_electricity_mix(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/electricity_maps/electricity_mix/latest"

    df_raw = (
        spark.read
        .schema(ELECTRICITY_MIX_RAW_SCHEMA)
        # multiLine=true: el JSON del mix tiene el array 'data' en múltiples líneas.
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
    )

    df_raw_exp = (
        df_raw
        .filter(F.col("data").isNotNull())
        .withColumn("row", F.explode("data"))
        .select(
            F.col("zone"),
            F.col("unit"),
            F.col("temporalGranularity").alias("temporal_granularity"),
            F.try_to_timestamp(
                F.col("row.datetime"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).alias("datetime"),
            F.col("row.nuclear").alias("nuclear_mw"),
            F.col("row.geothermal").alias("geothermal_mw"),
            F.col("row.biomass").alias("biomass_mw"),
            F.col("row.coal").alias("coal_mw"),
            F.col("row.wind").alias("wind_mw"),
            F.col("row.solar").alias("solar_mw"),
            F.col("row.hydro").alias("hydro_mw"),
            F.col("row.gas").alias("gas_mw"),
            F.col("row.oil").alias("oil_mw"),
            F.col("row.unknown").alias("unknown_mw"),
            F.col("row.hydro_discharge").alias("hydro_discharge_mw"),
            F.col("row.battery_discharge").alias("battery_discharge_mw"),
        )
        .withColumn("year",  F.year("datetime"))
        .withColumn("month", F.month("datetime"))
    )

    # ── SELF-HEALING: filtrar filas con datetime nulo (cast fallido)
    df_clean = df_raw_exp.filter(F.col("datetime").isNotNull())
    df = _log_dropped(df_raw_exp, df_clean, "electricity_mix/latest")

    result: WriteResult = write_to_silver(df, "electricity_maps/electricity_mix/latest")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 4. Zonas observadas derivadas de endpoints activos
# ---------------------------------------------------------------------------

def process_observed_zones_dimension(spark: SparkSession) -> tuple[DataFrame, int]:
    # Fuentes con cobertura real de zonas observadas en producción.
    ci_latest = (
        spark.read.schema(CARBON_INTENSITY_FLAT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(f"{BRONZE}/electricity_maps/carbon_intensity/latest")
        .select(F.col("zone").alias("zone_key"))
    )
    ci_past = (
        spark.read.schema(CARBON_INTENSITY_FLAT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(f"{BRONZE}/electricity_maps/carbon_intensity/past")
        .select(F.col("zone").alias("zone_key"))
    )
    ci_history = (
        spark.read.schema(CARBON_INTENSITY_HISTORY_RAW_SCHEMA)
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{BRONZE}/electricity_maps/carbon_intensity/history")
        .withColumn("event", F.explode("history"))
        .select(F.col("event.zone").alias("zone_key"))
    )
    mix_latest = (
        spark.read.schema(ELECTRICITY_MIX_RAW_SCHEMA)
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(f"{BRONZE}/electricity_maps/electricity_mix/latest")
        .select(F.col("zone").alias("zone_key"))
    )

    observed_zones = (
        ci_latest.unionByName(ci_past)
        .unionByName(ci_history)
        .unionByName(mix_latest)
        .filter(F.col("zone_key").isNotNull())
        .dropDuplicates(["zone_key"])
    )

    geo_mapping = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{BRONZE}/reference/geo_cloud_to_country_and_zones.csv")
        .select(
            F.col("electricity_maps_zone").alias("zone_key"),
            "iso_alpha2",
            "iso_alpha3",
            "country_name_mlco2",
            "country_name_global_petrol",
        )
        .dropDuplicates(["zone_key"])
    )

    df = (
        observed_zones.alias("z")
        .join(geo_mapping.alias("m"), on="zone_key", how="left")
        .withColumn("zone_name", F.lit(None).cast("string"))
        .withColumn(
            "country_name",
            F.coalesce(F.col("m.country_name_mlco2"), F.col("m.country_name_global_petrol")),
        )
        .withColumn("country_code", F.col("m.iso_alpha2"))
        .withColumn("source", F.lit("observed_from_carbon_mix_and_geo_mapping"))
        .select(
            "zone_key",
            "zone_name",
            "country_name",
            "country_code",
            "iso_alpha2",
            "iso_alpha3",
            "source",
        )
    )

    result = write_to_silver(df, "reference/zones_dimension")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 5. Global Petrol Prices + columna precio_red_estimado_usd
# ---------------------------------------------------------------------------

def process_global_petrol_prices(spark: SparkSession) -> tuple[DataFrame, int]:
    """
    Lee el CSV de precios estáticos 2023-2026 y agrega la columna derivada
    precio_red_estimado_usd basada en reglas de estacionalidad simuladas.

    NOTA: precio_red_estimado_usd es una imputación sintética basada en
    reglas de negocio ante la ausencia de series de tiempo de precios reales.
    Consultar DOCS_SILVER.md para descripción completa de la heurística.
    """
    bronze_path = f"{BRONZE}/global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv"

    df = (
        spark.read
        .option("header", "true")
        .schema(GLOBAL_PETROL_PRICES_SCHEMA)
        .csv(bronze_path)
    )

    # --- Estacionalidad simulada ---
    # Usamos el mes actual de ingesta como proxy del ciclo estacional.
    # La columna precio_red_estimado_usd aplica un factor sobre la tarifa
    # residencial estática según la siguiente heurística:
    #   - Meses de verano del hemisferio Norte (jun-ago): +15% (máx. AC)
    #   - Meses de invierno del hemisferio Norte (dic-feb): +12% (calefacción)
    #   - Resto del año (temporada media):                  +5%  (baseline)
    # La señal de estacionalidad es global (no distingue hemisferio Sur por
    # ausencia de esa dimensión en los datos estáticos actuales).
    ingestion_month = F.month(F.current_timestamp())

    seasonal_factor = (
        F.when(ingestion_month.isin(6, 7, 8), F.lit(1.15))
         .when(ingestion_month.isin(12, 1, 2), F.lit(1.12))
         .otherwise(F.lit(1.05))
    )

    df = df.withColumn(
        "precio_red_estimado_usd",
        F.round(F.col("residential_usd_per_kwh") * seasonal_factor, 4)
    )

    result = write_to_silver(df, "global_petrol_prices")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 6. MLCO2 — Yearly Averages
# ---------------------------------------------------------------------------

def process_mlco2_yearly_avg(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/mlco2/2021-10-27yearly_averages.csv"

    df = (
        spark.read
        .option("header", "true")
        .schema(MLCO2_YEARLY_AVG_SCHEMA)
        .csv(bronze_path)
        .withColumnRenamed("Zone Name", "zone_name")
        .withColumnRenamed("Country", "country")
    )

    result: WriteResult = write_to_silver(df, "mlco2/yearly_averages")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 6B. MLCO2 — Otros Catálogos
# ---------------------------------------------------------------------------
def process_mlco2_instances(spark: SparkSession) -> tuple[DataFrame, int]:
    df_raw = spark.read.option("header", "true").schema(MLCO2_INSTANCES_SCHEMA).csv(f"{BRONZE}/mlco2/instances.csv")
    df_clean = df_raw.filter(F.col("id").isNotNull())
    df = _log_dropped(df_raw, df_clean, "mlco2/instances")
    result: WriteResult = write_to_silver(df, "mlco2/instances")
    return df, result.rows_written

def process_mlco2_impact(spark: SparkSession) -> tuple[DataFrame, int]:
    df_raw = spark.read.option("header", "true").schema(MLCO2_IMPACT_SCHEMA).csv(f"{BRONZE}/mlco2/impact.csv")
    df_renamed = (df_raw.withColumnRenamed("providerName", "provider_name")
                        .withColumnRenamed("offsetRatio", "offset_ratio")
                        .withColumnRenamed("regionName", "region_name")
                        .withColumnRenamed("PUE", "pue")
                        .withColumnRenamed("PUE source", "pue_source"))
    df_clean = df_renamed.filter(F.col("region").isNotNull())
    df = _log_dropped(df_renamed, df_clean, "mlco2/impact")
    result: WriteResult = write_to_silver(df, "mlco2/impact")
    return df, result.rows_written

def process_mlco2_gpus(spark: SparkSession) -> tuple[DataFrame, int]:
    df_raw = spark.read.option("header", "true").schema(MLCO2_GPUS_SCHEMA).csv(f"{BRONZE}/mlco2/gpus.csv")
    df_renamed = (df_raw.withColumnRenamed("name", "gpu_model")
                        .withColumnRenamed("TFLOPS32", "tflops_32")
                        .withColumnRenamed(" TFLOPS16", "tflops_16")
                        .withColumnRenamed("GFLOPS32/W", "gflops_32_per_w")
                        .withColumnRenamed("GFLOPS16/W", "gflops_16_per_w"))
    df_clean = df_renamed.filter(F.col("gpu_model").isNotNull())
    df = _log_dropped(df_renamed, df_clean, "mlco2/gpus")
    result: WriteResult = write_to_silver(df, "mlco2/gpus")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 7. OWID — Energy Data
# ---------------------------------------------------------------------------

def process_owid(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/owid/owid-energy-data.csv"

    # solo las columnas relevantes, ignorando el resto de las 130 columnas originales.
    df_raw = (
        spark.read
        .option("header", "true")
        .schema(OWID_ENERGY_SCHEMA)
        .csv(bronze_path)
    )

    select_exprs = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in OWID_ENERGY_SCHEMA.fields
    ]
    df_casted = df_raw.select(*select_exprs)

    # ── SELF-HEALING: Filtros de calidad ─────────────────────────────────────
    # 1. year no nulo (clave de partición)
    # 2. country no nulo y no vacío (clave de cruce con otras tablas)
    # 3. iso_code no nulo (inyectado ~3% por stress test)
    # 4. carbon_intensity_elec >= 0 cuando presente (negativo = corrupto)
    df_clean = df_casted.filter(
        F.col("year").isNotNull() &
        F.col("country").isNotNull() & (F.trim(F.col("country")) != "") &
        # F.col("iso_code").isNotNull() &
        (
            F.col("carbon_intensity_elec").isNull() |
            (F.col("carbon_intensity_elec") >= 0)
        )
    )
    df = _log_dropped(df_raw, df_clean, "owid")

    result: WriteResult = write_to_silver(df, "owid")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 8. Reference — EC2 Pricing
# ---------------------------------------------------------------------------

def process_ec2_pricing(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/reference/aws_ec2_on_demand_usd_per_hour.csv"

    df = (
        spark.read
        .option("header", "true")
        .schema(EC2_PRICING_SCHEMA)
        .csv(bronze_path)
        .withColumn("as_of_date", F.to_date("as_of_date", "yyyy-MM-dd"))
    )

    result: WriteResult = write_to_silver(df, "reference/ec2_pricing")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 9. Geo Cloud Mapping (Tabla Puente Custom)
# ---------------------------------------------------------------------------

def process_geo_cloud_mapping(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/reference/geo_cloud_to_country_and_zones.csv"

    df_raw = (
        spark.read
        .option("header", "true")
        .schema(GEO_CLOUD_MAPPING_SCHEMA)
        .csv(bronze_path)
    )

    # ── SELF-HEALING: Filtros de integridad ──────────────────────────────────
    # Aseguramos que las llaves puente no vengan nulas.
    df_clean = df_raw.filter(
        F.col("cloud_provider").isNotNull() &
        F.col("cloud_region").isNotNull() &
        F.col("electricity_maps_zone").isNotNull()
    )
    df = _log_dropped(df_raw, df_clean, "reference/geo_cloud_mapping")

    result: WriteResult = write_to_silver(df, "reference/geo_cloud_mapping")
    return df, result.rows_written

# ---------------------------------------------------------------------------
# 10. Usage Logs (sintético)
# ---------------------------------------------------------------------------

def process_usage_logs(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/usage_logs/usage_logs.csv"

    df_raw = (
        spark.read
        .option("header", "true")
        .schema(USAGE_LOGS_SCHEMA)
        .csv(bronze_path)
        # ── SELF-HEALING: Safe cast de timestamp ────────────────────────────
        # Con ansi.enabled=false, strings inválidos ("N/A", "", "20260145")
        # retornan null. Luego filtramos la clave primaria timestamp != null.
        .withColumn("timestamp", _safe_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("year",  F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
    )

    # ── SELF-HEALING: Filtros de dominio ─────────────────────────────────────
    # CA Jira: energy_consumed_kwh > 0
    # Adicionales: clave primaria timestamp no nula, duration > 0, gpu_util en [0,1]
    VALID_JOB_TYPES = {"Training", "Inference", "Fine-tuning"}
    VALID_STATUSES  = {"Success", "Failed"}

    df_clean = df_raw.filter(
        F.col("timestamp").isNotNull()                       &  # clave primaria
        (F.col("energy_consumed_kwh") > 0)                  &  # CA Jira
        (F.col("duration_hours") > 0)                       &  # duración lógica
        (F.col("gpu_utilization") >= 0)                     &  # mínimo físico
        (F.col("gpu_utilization") <= 1)                     &  # máximo físico
        F.col("job_type").isin(*VALID_JOB_TYPES)            &  # dominio permitido
        F.col("execution_status").isin(*VALID_STATUSES)        # dominio permitido
    )
    df = _log_dropped(df_raw, df_clean, "usage_logs")

    result: WriteResult = write_to_silver(df, "usage_logs")
    return df, result.rows_written


# ---------------------------------------------------------------------------
# 11. World Bank — ICT Service Exports
# ---------------------------------------------------------------------------

def process_world_bank(spark: SparkSession) -> tuple[DataFrame, int]:
    """
    Regla Especial: El CSV tiene 4 filas de metadatos antes del header real.
    Se usa PySpark con header=true y la opción de skip simulada leyendo en
    Pandas primero con skiprows=4, luego convirtiendo.

    La tabla se transforma de formato ancho (1 col por año 1960-2025) a
    formato largo (country_code, year, ict_exports_usd) mediante melt/stack.
    """
    import pandas as pd
    import boto3, io

    bronze_key = "world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv"
    temp_s3_key = "world_bank/.temp/temp_processed_ict.csv"

    # Leer con skiprows desde S3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=bronze_key)
    body = obj["Body"].read()

    pdf = pd.read_csv(io.BytesIO(body), skiprows=4, dtype=str)

    # Columnas de año
    year_cols = [c for c in pdf.columns if c.strip().isdigit()
                 and 1960 <= int(c.strip()) <= 2025]

    id_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
    pdf_long = pdf.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year",
        value_name="ict_exports_usd"
    )

    pdf_long.rename(columns={
        "Country Name": "country_name",
        "Country Code": "country_code",
        "Indicator Name": "indicator_name",
        "Indicator Code": "indicator_code",
    }, inplace=True)

    pdf_long["year"] = pdf_long["year"].astype(int)
    pdf_long["ict_exports_usd"] = pd.to_numeric(pdf_long["ict_exports_usd"], errors="coerce")
    pdf_long = pdf_long.dropna(subset=["ict_exports_usd"])

    # Escribir en buffer y subir a S3
    csv_buffer = io.StringIO()
    pdf_long.to_csv(csv_buffer, index=False)
    
    # Subimos el CSV limpio temporalmente a S3
    s3.put_object(Bucket=BRONZE_BUCKET, Key=temp_s3_key, Body=csv_buffer.getvalue())

    try:
        # Ahora TODOS los workers de Spark pueden ver el archivo
        df = (
            spark.read
            .option("header", "true")
            .schema(WORLD_BANK_ICT_SCHEMA)
            .csv(f"s3a://{BRONZE_BUCKET}/{temp_s3_key}")
        )

        result = write_to_silver(df, "world_bank/ict_exports")
        return df, result.rows_written

    finally:
        # Limpieza: Borramos el archivo temporal de S3 para no dejar basura
        try:
            s3.delete_object(Bucket=BRONZE_BUCKET, Key=temp_s3_key)
            logger.info("[QA:world_bank/ict_exports] Archivo temporal S3 eliminado.")
        except Exception as e:
            logger.warning(f"No se pudo eliminar el temporal en S3: {e}")

# ---------------------------------------------------------------------------
# 12. World Bank Metadata (Tabla de Dimensión)
# ---------------------------------------------------------------------------

def process_world_bank_metadata(spark: SparkSession) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE}/world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv"

    # Este CSV no tiene filas basura al inicio, se lee normal.
    df_raw = (
        spark.read
        .option("header", "true")
        .schema(WORLD_BANK_METADATA_SCHEMA)
        .csv(bronze_path)
    )

    # Renombrar a snake_case y quitar columna vacía del final ("Unnamed: 5")
    df_renamed = (
        df_raw
        .withColumnRenamed("Country Code", "country_code")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("IncomeGroup", "income_group")
        .withColumnRenamed("SpecialNotes", "special_notes")
        .withColumnRenamed("TableName", "table_name")
    )
    if "Unnamed: 5" in df_renamed.columns:
        df_renamed = df_renamed.drop("Unnamed: 5")

    # ── SELF-HEALING: country_code no puede ser nulo y debe ser único
    df_clean = (
        df_renamed
        .filter(F.col("country_code").isNotNull())
        .dropDuplicates(["country_code"])
    )
    df = _log_dropped(df_renamed, df_clean, "reference/world_bank_metadata")

    result: WriteResult = write_to_silver(df, "reference/world_bank_metadata")
    return df, result.rows_written

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    spark = build_spark()
    results = {}

    logger.info("=" * 60)
    logger.info(" GREEN AI — Bronze → Silver ETL")
    logger.info("=" * 60)

    steps = [
        ("electricity_maps/carbon_intensity/latest",  lambda: process_carbon_intensity_flat(spark, "latest")),
        ("electricity_maps/carbon_intensity/past",    lambda: process_carbon_intensity_flat(spark, "past")),
        ("electricity_maps/carbon_intensity/history", lambda: process_carbon_intensity_history(spark)),
        ("electricity_maps/electricity_mix/latest",   lambda: process_electricity_mix(spark)),
        ("reference/zones_dimension",                 lambda: process_observed_zones_dimension(spark)),
        ("global_petrol_prices",                      lambda: process_global_petrol_prices(spark)),
        ("mlco2/yearly_averages",                     lambda: process_mlco2_yearly_avg(spark)),
        ("mlco2/instances",                           lambda: process_mlco2_instances(spark)),
        ("mlco2/impact",                              lambda: process_mlco2_impact(spark)),
        ("mlco2/gpus",                                lambda: process_mlco2_gpus(spark)),
        ("owid",                                      lambda: process_owid(spark)),
        ("reference/ec2_pricing",                     lambda: process_ec2_pricing(spark)),
        ("reference/geo_cloud_mapping",               lambda: process_geo_cloud_mapping(spark)),
        ("usage_logs",                                lambda: process_usage_logs(spark)),
        ("world_bank/ict_exports",                    lambda: process_world_bank(spark)),
        ("reference/world_bank_metadata",             lambda: process_world_bank_metadata(spark)),
    ]

    for name, fn in steps:
        try:
            logger.info("[>] Procesando: %s ...", name)
            _, count = fn()
            results[name] = {"status": "OK", "rows_written": count}
            logger.info("    OK  %s — %d filas escritas en Silver.", name, count)
        except Exception as exc:
            results[name] = {"status": "ERROR", "error": str(exc)}
            logger.error("    ERROR  %s — %s", name, exc)

    logger.info("=" * 60)
    logger.info(" Resumen de ejecucion")
    logger.info("=" * 60)
    for name, r in results.items():
        status = r["status"]
        detail = r.get("rows_written", r.get("error"))
        logger.info("  %-6s  %-35s %s", status, name, detail)

    spark.stop()


if __name__ == "__main__":
    main()
