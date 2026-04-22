"""
silver_to_gold.py
==================
Capa Gold — Modelo Dimensional Kimball (Esquema Estrella)
Plataforma: Green AI Analytics Platform

Parte 1: Configuración de Spark e idempotencia + funciones de dimensiones.
Parte 2: Funciones de hechos (a completar en la siguiente iteración).

Convenciones:
  - Surrogate Keys: md5() sobre la clave natural en lowercase / trimmed.
  - Escritura de dimensiones: mode("overwrite") total (sin partición).
  - Escritura de hechos: mode("overwrite") con partitionBy() + dynamic overwrite.
  - Tipos estrictos en todos los casts de salida.
  - Bloques try/except por función para aislamiento de fallos.

Buckets:
  - Silver: s3a://green-ai-pf-silver-a0e96d06/
  - Gold:   s3a://green-ai-pf-gold-a0e96d06/
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.gold")

# =============================================================================
# Dependencias opcionales de desarrollo
# =============================================================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Producción: credenciales via IAM Role / Airflow env

# =============================================================================
# Constantes de Infraestructura
# =============================================================================

SILVER_BUCKET: str = os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")
GOLD_BUCKET: str   = os.getenv("S3_GOLD_BUCKET",   "green-ai-pf-gold-a0e96d06")

SILVER: str = f"s3a://{SILVER_BUCKET}"
GOLD: str   = f"s3a://{GOLD_BUCKET}"

# =============================================================================
# Inicialización de SparkSession
# CRÍTICO: partitionOverwriteMode=dynamic garantiza idempotencia estricta.
# Las reescrituras parciales no borran particiones ajenas al lote actual.
# =============================================================================

spark: SparkSession = (
    SparkSession.builder
    .appName("green-ai-silver-to-gold")
    # ── Idempotencia estricta (CRÍTICO) ───────────────────────────────────────
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # ── Zona horaria canónica ─────────────────────────────────────────────────
    .config("spark.sql.session.timeZone", "UTC")
    # ── Delta Lake ────────────────────────────────────────────────────────────
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # ── JARs: Delta + Hadoop-AWS + SDK ────────────────────────────────────────
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "software.amazon.awssdk:bundle:2.20.18",
    )
    # ── Credenciales AWS ─────────────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    )
    # ── Fix Hadoop 3.3.4 «60s» NumberFormatException ─────────────────────────
    .config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime",        "60")
    .config("spark.hadoop.fs.s3a.multipart.purge",              "false")
    .config("spark.hadoop.fs.s3a.multipart.purge.age",          "86400")
    # ── Optimizaciones de escritura S3 ────────────────────────────────────────
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    # ── Memoria del driver ────────────────────────────────────────────────────
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info("SparkSession iniciada. partitionOverwriteMode=dynamic ✓")

# =============================================================================
# PARTE 1 — FUNCIONES DE DIMENSIONES (Kimball)
# =============================================================================
# Cada función:
#   1. Lee la(s) tabla(s) Silver necesarias.
#   2. Genera la Surrogate Key con md5() sobre la clave natural.
#   3. Selecciona y castea las columnas finales con tipos estrictos.
#   4. Escribe en Gold con mode("overwrite") TOTAL (sin partición, tablas chicas).
#   5. Retorna el DataFrame Gold para uso posterior en los JOINs de hechos.
# =============================================================================


def build_dim_country() -> DataFrame:
    """
    Dimensión de país.

    Fuentes Silver:
      - owid/                          → iso_code, country (nombre)
      - reference/world_bank_metadata/ → region, income_group (via country_code)

    Surrogate Key: md5(iso_alpha3)
    Destino Gold:  s3a://<gold>/dim_country/
    """
    logger.info("[dim_country] Iniciando construcción...")
    try:
        # ── 1. Leer fuentes Silver ────────────────────────────────────────────
        owid: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/owid")
            .select(
                F.trim(F.col("country")).alias("country_name"),
                F.upper(F.trim(F.col("iso_code"))).alias("iso_alpha3"),
            )
            .filter(F.col("iso_alpha3").isNotNull() & (F.col("iso_alpha3") != ""))
            .dropDuplicates(["iso_alpha3"])
        )

        wb_meta: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/world_bank_metadata")
            .select(
                F.upper(F.trim(F.col("country_code"))).alias("iso_alpha3"),
                F.trim(F.col("region")).alias("wb_region"),
                F.trim(F.col("income_group")).alias("income_group"),
            )
        )

        # ── 2. JOIN por iso_alpha3 ────────────────────────────────────────────
        dim: DataFrame = owid.join(wb_meta, on="iso_alpha3", how="left")

        # ── 3. Surrogate Key + columnas finales ───────────────────────────────
        dim = dim.withColumn(
            "country_id",
            F.md5(F.lower(F.trim(F.col("iso_alpha3")))),
        ).select(
            F.col("country_id").cast(StringType()),
            F.col("country_name").cast(StringType()),
            F.col("iso_alpha3").cast(StringType()),
            F.col("wb_region").cast(StringType()).alias("region"),
            F.col("income_group").cast(StringType()),
        )

        # ── 4. Escribir en Gold (overwrite total; tabla pequeña) ──────────────
        (
            dim.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{GOLD}/dim_country")
        )

        row_count: int = dim.count()
        logger.info("[dim_country] OK — %d filas escritas en Gold.", row_count)
        return dim

    except Exception as exc:
        logger.error("[dim_country] ERROR: %s", exc, exc_info=True)
        raise


def build_dim_region() -> DataFrame:
    """
    Dimensión puente de región cloud.

    Fuente Silver:
      - reference/geo_cloud_mapping/

    Columnas clave del esquema Silver (GEO_CLOUD_MAPPING_SCHEMA):
      cloud_provider, cloud_region, region_name_mlco2, country_name_mlco2,
      iso_alpha2, iso_alpha3, electricity_maps_zone, is_primary_zone,
      mapping_notes.

    Surrogate Key: md5(lower(cloud_provider) || '|' || lower(cloud_region))
    Destino Gold:  s3a://<gold>/dim_region/
    """
    logger.info("[dim_region] Iniciando construcción...")
    try:
        # ── 1. Leer fuente Silver ─────────────────────────────────────────────
        geo: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/geo_cloud_mapping")
        )

        # ── 2. Surrogate Key de región ────────────────────────────────────────
        dim: DataFrame = geo.withColumn(
            "region_id",
            F.md5(
                F.concat_ws(
                    "|",
                    F.lower(F.trim(F.col("cloud_provider"))),
                    F.lower(F.trim(F.col("cloud_region"))),
                )
            ),
        )

        # FK a dim_country (generada con la misma fórmula de md5 sobre iso_alpha3)
        dim = dim.withColumn(
            "country_id",
            F.when(
                F.col("iso_alpha3").isNotNull() & (F.col("iso_alpha3") != ""),
                F.md5(F.lower(F.trim(F.col("iso_alpha3")))),
            ).otherwise(F.lit(None).cast(StringType())),
        )

        # ── 3. Selección y cast final ─────────────────────────────────────────
        dim = dim.select(
            F.col("region_id").cast(StringType()),
            F.col("cloud_provider").cast(StringType()),
            F.col("cloud_region").cast(StringType()).alias("aws_region_code"),
            F.col("region_name_mlco2").cast(StringType()).alias("aws_region_name"),
            F.col("electricity_maps_zone").cast(StringType()),
            F.col("country_name_mlco2").cast(StringType()).alias("country_name"),
            F.col("country_id").cast(StringType()),
            F.col("iso_alpha2").cast(StringType()),
            F.col("iso_alpha3").cast(StringType()),
            F.col("is_primary_zone").cast("boolean"),
            F.col("mapping_notes").cast(StringType()),
        )

        # ── 4. Escribir en Gold ───────────────────────────────────────────────
        (
            dim.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{GOLD}/dim_region")
        )

        row_count: int = dim.count()
        logger.info("[dim_region] OK — %d filas escritas en Gold.", row_count)
        return dim

    except Exception as exc:
        logger.error("[dim_region] ERROR: %s", exc, exc_info=True)
        raise


def build_dim_gpu_model() -> DataFrame:
    """
    Dimensión catálogo de GPUs (MLCO2).

    Fuente Silver:
      - mlco2/gpus/

    Columnas clave del esquema Silver (MLCO2_GPUS_SCHEMA):
      gpu_model (renombrado desde 'name' en Silver),
      type, tdp_watts, TFLOPS32, TFLOPS16, memory.

    NOTA: La capa Silver renombra 'name' → 'gpu_model' (ver bronze_to_silver.py).
          Se mapean TFLOPS32 → gflops_fp32 y TFLOPS16 → gflops_fp16 para
          alinearse con el modelo dimensional (dimensional_model.md §dim_gpu_model).

    Surrogate Key: md5(lower(trim(gpu_model)))
    Destino Gold:  s3a://<gold>/dim_gpu_model/
    """
    logger.info("[dim_gpu_model] Iniciando construcción...")
    try:
        # ── 1. Leer fuente Silver ─────────────────────────────────────────────
        gpus: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/mlco2/gpus")
        )

        # ── 2. Surrogate Key ──────────────────────────────────────────────────
        dim: DataFrame = gpus.withColumn(
            "gpu_id",
            F.md5(F.lower(F.trim(F.col("gpu_model")))),
        )

        # ── 3. Normalizar nombres de columnas TFLOPS ──────────────────────────
        # El esquema bronze usa 'TFLOPS32' y ' TFLOPS16' (con espacio).
        # La capa Silver debería haberlos limpiado; se aplica alias defensivo.
        tflops32_col: str = "TFLOPS32" if "TFLOPS32" in gpus.columns else "tflops32"
        tflops16_col: str = (
            " TFLOPS16" if " TFLOPS16" in gpus.columns
            else "TFLOPS16" if "TFLOPS16" in gpus.columns
            else "tflops16"
        )

        # ── 4. Selección y cast estricto ──────────────────────────────────────
        dim = dim.select(
            F.col("gpu_id").cast(StringType()),
            F.col("gpu_model").cast(StringType()),
            F.col("type").cast(StringType()).alias("gpu_type"),
            F.col("tdp_watts").cast(IntegerType()),
            F.col(tflops32_col).cast(DoubleType()).alias("gflops_fp32"),
            F.col(tflops16_col).cast(DoubleType()).alias("gflops_fp16"),
            F.col("memory").cast(StringType()).alias("memory_spec"),
        )

        # ── 5. Escribir en Gold ───────────────────────────────────────────────
        (
            dim.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{GOLD}/dim_gpu_model")
        )

        row_count: int = dim.count()
        logger.info("[dim_gpu_model] OK — %d filas escritas en Gold.", row_count)
        return dim

    except Exception as exc:
        logger.error("[dim_gpu_model] ERROR: %s", exc, exc_info=True)
        raise


def build_dim_instance_type() -> DataFrame:
    """
    Dimensión de tipos de instancia EC2 (proxy TCO de cómputo).

    Fuente Silver:
      - reference/ec2_pricing/

    Columnas clave del esquema Silver (EC2_PRICING_SCHEMA):
      cloud_provider, cloud_region, instance_type, operating_system,
      tenancy, pricing_model, currency, price_usd_per_hour,
      price_basis_region, regional_multiplier, as_of_date, source,
      pricing_notes.

    Surrogate Key: md5(lower(instance_type) || '|' || lower(cloud_region))
    Destino Gold:  s3a://<gold>/dim_instance_type/
    """
    logger.info("[dim_instance_type] Iniciando construcción...")
    try:
        # ── 1. Leer fuente Silver ─────────────────────────────────────────────
        ec2: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/ec2_pricing")
        )

        # ── 2. Surrogate Key de instancia (instance_type + región) ────────────
        dim: DataFrame = ec2.withColumn(
            "instance_type_id",
            F.md5(
                F.concat_ws(
                    "|",
                    F.lower(F.trim(F.col("instance_type"))),
                    F.lower(F.trim(F.col("cloud_region"))),
                )
            ),
        )

        # ── 3. Selección y cast estricto ──────────────────────────────────────
        dim = dim.select(
            F.col("instance_type_id").cast(StringType()),
            F.col("cloud_provider").cast(StringType()).alias("provider"),
            F.col("cloud_region").cast(StringType()),
            F.col("instance_type").cast(StringType()),
            F.col("operating_system").cast(StringType()).alias("os_type"),
            F.col("price_usd_per_hour").cast(DoubleType()),
            F.col("pricing_model").cast(StringType()),
            F.col("as_of_date").cast(StringType()).alias("price_source_date"),
            F.col("source").cast(StringType()),
            F.col("pricing_notes").cast(StringType()).alias("notes"),
        )

        # ── 4. Escribir en Gold ───────────────────────────────────────────────
        (
            dim.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{GOLD}/dim_instance_type")
        )

        row_count: int = dim.count()
        logger.info("[dim_instance_type] OK — %d filas escritas en Gold.", row_count)
        return dim

    except Exception as exc:
        logger.error("[dim_instance_type] ERROR: %s", exc, exc_info=True)
        raise


def build_dim_electricity_price() -> DataFrame:
    """
    Dimensión de precio eléctrico residencial estático por país.

    Fuente Silver:
      - global_petrol_prices/

    Columnas clave del esquema Silver (GLOBAL_PETROL_PRICES_SCHEMA):
      country, residential_usd_per_kwh, business_usd_per_kwh.

    Diseño: precio estático (sin granularidad temporal mes/año); la columna
    'year' y 'month' se dejan como NULL para indicar precio de referencia
    general (puede actualizarse en Parte 2 si se agrega partición temporal).

    Surrogate Key: md5(lower(trim(country)))
    Destino Gold:  s3a://<gold>/dim_electricity_price/
    """
    logger.info("[dim_electricity_price] Iniciando construcción...")
    try:
        # ── 1. Leer fuente Silver ─────────────────────────────────────────────
        prices: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/global_petrol_prices")
        )

        # ── 2. Surrogate Key de precio por país ───────────────────────────────
        dim: DataFrame = prices.withColumn(
            "price_id",
            F.md5(F.lower(F.trim(F.col("country")))),
        )

        # FK a dim_country — la clave natural compartida es country_name;
        # se genera aquí para facilitar el JOIN posterior en los hechos.
        # NOTA: country de global_petrol_prices NO tiene iso_alpha3 directo;
        # la resolución de FK se realizará en las tablas de hechos via dim_region.
        dim = dim.select(
            F.col("price_id").cast(StringType()),
            F.col("country").cast(StringType()).alias("country_name"),
            F.lit(None).cast(IntegerType()).alias("year"),
            F.lit(None).cast(IntegerType()).alias("month"),
            F.col("residential_usd_per_kwh").cast(DoubleType()).alias("price_usd_per_kwh"),
            F.lit("residential").cast(StringType()).alias("price_type"),
            F.lit("global_petrol_prices").cast(StringType()).alias("price_source"),
            F.lit(None).cast(StringType()).alias("price_reference_date"),
        )

        # ── 3. Escribir en Gold ───────────────────────────────────────────────
        (
            dim.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{GOLD}/dim_electricity_price")
        )

        row_count: int = dim.count()
        logger.info("[dim_electricity_price] OK — %d filas escritas en Gold.", row_count)
        return dim

    except Exception as exc:
        logger.error("[dim_electricity_price] ERROR: %s", exc, exc_info=True)
        raise


# =============================================================================
# PARTE 2 — TABLAS DE HECHOS (Kimball)
# =============================================================================
# Convenciones de esta sección:
#   - job_type y execution_status son DIMENSIONES DEGENERADAS: sus strings se
#     mantienen directamente en la tabla de hechos (no se crean dim_ separadas).
#   - Los JOINs con dimensiones usan las mismas fórmulas md5() de la Parte 1
#     para regenerar la surrogate key de lookup sin leer las tablas Gold.
#   - coalesce() se usa SOLO como guardia matemática (evitar NULL * número),
#     nunca para imputar valores de negocio inventados.
#   - Escritura de hechos: mode("overwrite") + partitionBy() + dynamic overwrite
#     (configurado en la SparkSession).
# =============================================================================


def build_fact_ai_compute_usage() -> DataFrame:
    """
    Tabla de hechos principal de sesiones de cómputo de IA.

    Fuente Silver:
      - usage_logs/                    → tabla base de sesiones
      - reference/geo_cloud_mapping/   → lookup de region → iso_alpha3
      - reference/ec2_pricing/         → precio compute por (region, instance)
      - global_petrol_prices/          → precio eléctrico residencial por país

    Dimensiones degeneradas (se mantienen como strings en la fila de hecho):
      - job_type         (Training / Inference / Fine-tuning)
      - execution_status (Success / Failed)

    Surrogate Keys generadas localmente (mismo md5 que las dim_*):
      - region_id        → md5(cloud_provider|cloud_region)
      - instance_type_id → md5(instance_type|cloud_region)
      - price_id         → md5(country_petrol)
      - gpu_id           → md5(gpu_model)

    Métricas calculadas:
      - cost_electricity_usd = energy_consumed_kwh * residential_usd_per_kwh
      - cost_compute_usd     = duration_hours * price_usd_per_hour

    Partición física: year, month (extraídos del timestamp del log).
    Destino Gold: s3a://<gold>/fact_ai_compute_usage/
    """
    logger.info("[fact_ai_compute_usage] Iniciando construcción...")
    try:
        # ── 1. Fuente principal: usage_logs ───────────────────────────────────
        logs: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/usage_logs")
            .withColumn("ts", F.to_timestamp(F.col("timestamp")))
            .withColumn("year",  F.year(F.col("ts")).cast(IntegerType()))
            .withColumn("month", F.month(F.col("ts")).cast(IntegerType()))
        )

        # ── 2. Lookup: geo_cloud_mapping → iso_alpha3 por cloud_region ────────
        # Usamos la misma clave normalizada que en build_dim_region().
        geo_lookup: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/geo_cloud_mapping")
            .select(
                F.lower(F.trim(F.col("cloud_region"))).alias("_geo_region_key"),
                F.upper(F.trim(F.col("iso_alpha3"))).alias("iso_alpha3"),
                F.col("country_name_mlco2").alias("country_name_geo"),
                F.col("cloud_provider"),
            )
            .dropDuplicates(["_geo_region_key"])
        )

        logs = logs.withColumn(
            "_region_key_log", F.lower(F.trim(F.col("region")))
        )
        logs = logs.join(geo_lookup, logs["_region_key_log"] == geo_lookup["_geo_region_key"], "left")

        # ── 3. Surrogate Keys generadas en línea ──────────────────────────────

        # region_id: md5(cloud_provider|cloud_region)  — mismo que dim_region
        logs = logs.withColumn(
            "region_id",
            F.when(
                F.col("cloud_provider").isNotNull() & F.col("region").isNotNull(),
                F.md5(F.concat_ws("|",
                    F.lower(F.trim(F.col("cloud_provider"))),
                    F.lower(F.trim(F.col("region"))),
                )),
            ).otherwise(F.lit(None).cast(StringType())),
        )

        # gpu_id: md5(gpu_model)  — mismo que dim_gpu_model
        logs = logs.withColumn(
            "gpu_id",
            F.when(
                F.col("gpu_model").isNotNull(),
                F.md5(F.lower(F.trim(F.col("gpu_model")))),
            ).otherwise(F.lit(None).cast(StringType())),
        )

        # instance_type_id: md5(instance_type|region)  — mismo que dim_instance_type
        logs = logs.withColumn(
            "instance_type_id",
            F.when(
                F.col("instance_type").isNotNull() & F.col("region").isNotNull(),
                F.md5(F.concat_ws("|",
                    F.lower(F.trim(F.col("instance_type"))),
                    F.lower(F.trim(F.col("region"))),
                )),
            ).otherwise(F.lit(None).cast(StringType())),
        )

        # ── 4. Lookup: ec2_pricing → price_usd_per_hour ───────────────────────
        ec2_lookup: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/ec2_pricing")
            .select(
                F.md5(F.concat_ws("|",
                    F.lower(F.trim(F.col("instance_type"))),
                    F.lower(F.trim(F.col("cloud_region"))),
                )).alias("_ec2_sk"),
                F.col("price_usd_per_hour").cast(DoubleType()),
            )
            .dropDuplicates(["_ec2_sk"])
        )

        logs = logs.join(ec2_lookup, logs["instance_type_id"] == ec2_lookup["_ec2_sk"], "left") \
                   .drop("_ec2_sk")

        # ── 5. Lookup: global_petrol_prices → residential_usd_per_kwh ─────────
        # El join se hace por country_name_geo (normalizado desde geo_cloud_mapping)
        # comparado con el campo 'country' de global_petrol_prices.
        prices_lookup: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/global_petrol_prices")
            .select(
                F.lower(F.trim(F.col("country"))).alias("_petrol_country_key"),
                F.col("residential_usd_per_kwh").cast(DoubleType()),
            )
            .dropDuplicates(["_petrol_country_key"])
        )

        logs = logs.withColumn(
            "_country_key_log",
            F.lower(F.trim(F.col("country_name_geo"))),
        )
        logs = logs.join(
            prices_lookup,
            logs["_country_key_log"] == prices_lookup["_petrol_country_key"],
            "left",
        ).drop("_petrol_country_key")

        # price_id: md5(country_petrol)  — mismo que dim_electricity_price
        logs = logs.withColumn(
            "price_id",
            F.when(
                F.col("_country_key_log").isNotNull(),
                F.md5(F.col("_country_key_log")),
            ).otherwise(F.lit(None).cast(StringType())),
        )

        # ── 6. Métricas calculadas ─────────────────────────────────────────────
        # coalesce SOLO como guardia para evitar NULL en la multiplicación;
        # el valor real del cálculo se guarda en nullable=True si ambos lados son NULL.
        logs = logs.withColumn(
            "cost_electricity_usd",
            F.when(
                F.col("energy_consumed_kwh").isNotNull() & F.col("residential_usd_per_kwh").isNotNull(),
                F.col("energy_consumed_kwh").cast(DoubleType()) * F.col("residential_usd_per_kwh"),
            ).otherwise(F.lit(None).cast(DoubleType())),
        ).withColumn(
            "cost_compute_usd",
            F.when(
                F.col("duration_hours").isNotNull() & F.col("price_usd_per_hour").isNotNull(),
                F.col("duration_hours").cast(DoubleType()) * F.col("price_usd_per_hour"),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )

        # ── 7. Selección y cast final ─────────────────────────────────────────
        fact: DataFrame = logs.select(
            # Claves naturales / degeneradas
            F.col("session_id").cast(StringType()),
            F.col("user_id").cast(StringType()),
            F.col("timestamp").cast(StringType()),
            F.col("ts").cast("timestamp").alias("event_timestamp"),
            F.col("year").cast(IntegerType()),
            F.col("month").cast(IntegerType()),
            # Surrogate Keys → FKs a dimensiones
            F.col("region_id").cast(StringType()),
            F.col("gpu_id").cast(StringType()),
            F.col("instance_type_id").cast(StringType()),
            F.col("price_id").cast(StringType()),
            # Dimensiones degeneradas (strings directos, sin tabla separada)
            F.col("job_type").cast(StringType()),
            F.col("execution_status").cast(StringType()),
            # Atributos de contexto
            F.col("gpu_model").cast(StringType()),
            F.col("instance_type").cast(StringType()),
            F.col("region").cast(StringType()),
            F.col("iso_alpha3").cast(StringType()),
            # Medidas
            F.col("duration_hours").cast(DoubleType()),
            F.col("gpu_utilization").cast(DoubleType()),
            F.col("energy_consumed_kwh").cast(DoubleType()),
            F.col("residential_usd_per_kwh").cast(DoubleType()).alias("price_kwh_usd"),
            F.col("price_usd_per_hour").cast(DoubleType()).alias("price_compute_usd_per_hour"),
            F.col("cost_electricity_usd").cast(DoubleType()),
            F.col("cost_compute_usd").cast(DoubleType()),
        )

        # ── 8. Escribir en Gold con partición temporal ────────────────────────
        (
            fact.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year", "month")
            .save(f"{GOLD}/fact_ai_compute_usage")
        )

        row_count: int = fact.count()
        logger.info("[fact_ai_compute_usage] OK — %d filas escritas en Gold.", row_count)
        return fact

    except Exception as exc:
        logger.error("[fact_ai_compute_usage] ERROR: %s", exc, exc_info=True)
        raise


def build_fact_carbon_intensity_hourly() -> DataFrame:
    """
    Tabla de hechos de intensidad de carbono horaria y mix eléctrico.

    Estrategia de lectura:
      Intenta primero el endpoint 'history' (más completo). Si falla,
      cae al endpoint 'latest' como respaldo. Ambos comparten el mismo
      esquema Silver plano post-explode.

    Fuentes Silver:
      - electricity_maps/carbon_intensity/history/   (preferido)
      - electricity_maps/carbon_intensity/latest/    (respaldo)

    Enriquecimiento:
      - region_id via md5(cloud_provider|cloud_region) desde geo_cloud_mapping,
        haciendo join por electricity_maps_zone ↔ zone del evento.

    Dimensión degenerada:
      - emission_factor_type (string directo, sin tabla separada)

    Partición física: year, month.
    Destino Gold: s3a://<gold>/fact_carbon_intensity_hourly/
    """
    logger.info("[fact_carbon_intensity_hourly] Iniciando construcción...")
    try:
        # ── 1. Leer fuente (history preferred, latest fallback) ────────────────
        ci: DataFrame | None = None
        for endpoint in ("history", "latest", "past"):
            path = f"{SILVER}/electricity_maps/carbon_intensity/{endpoint}"
            try:
                ci = spark.read.format("delta").load(path)
                logger.info("[fact_carbon_intensity_hourly] Fuente cargada: %s", endpoint)
                break
            except Exception:  # noqa: BLE001
                logger.warning("[fact_carbon_intensity_hourly] Path no disponible: %s", path)

        if ci is None:
            raise RuntimeError(
                "No se pudo leer ningún endpoint de carbon_intensity desde Silver."
            )

        # ── 2. Columnas temporales ─────────────────────────────────────────────
        ci = (
            ci
            .withColumn("event_ts", F.to_timestamp(F.col("datetime")))
            .withColumn("year",  F.year(F.col("event_ts")).cast(IntegerType()))
            .withColumn("month", F.month(F.col("event_ts")).cast(IntegerType()))
            .withColumn("hour",  F.hour(F.col("event_ts")).cast(IntegerType()))
        )

        # ── 3. Lookup de region_id via zone ↔ electricity_maps_zone ───────────
        geo_zone_lookup: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/reference/geo_cloud_mapping")
            .select(
                F.trim(F.col("electricity_maps_zone")).alias("_zone_key"),
                F.md5(F.concat_ws("|",
                    F.lower(F.trim(F.col("cloud_provider"))),
                    F.lower(F.trim(F.col("cloud_region"))),
                )).alias("region_id"),
                F.upper(F.trim(F.col("iso_alpha3"))).alias("iso_alpha3"),
            )
            .dropDuplicates(["_zone_key"])
        )

        ci = ci.join(
            geo_zone_lookup,
            F.trim(ci["zone"]) == geo_zone_lookup["_zone_key"],
            "left",
        ).drop("_zone_key")

        # ── 4. Selección y cast final ─────────────────────────────────────────
        fact: DataFrame = ci.select(
            # Claves
            F.col("zone").cast(StringType()),
            F.col("region_id").cast(StringType()),
            F.col("iso_alpha3").cast(StringType()),
            # Temporal
            F.col("event_ts").cast("timestamp"),
            F.col("year").cast(IntegerType()),
            F.col("month").cast(IntegerType()),
            F.col("hour").cast(IntegerType()),
            # Dimensión degenerada
            F.col("emissionFactorType").cast(StringType()).alias("emission_factor_type"),
            F.col("isEstimated").cast("boolean").alias("is_estimated"),
            # Medida principal
            F.col("carbonIntensity").cast(DoubleType()).alias("carbon_intensity_gco2eq_per_kwh"),
        )

        # ── 5. Escribir en Gold ───────────────────────────────────────────────
        (
            fact.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year", "month")
            .save(f"{GOLD}/fact_carbon_intensity_hourly")
        )

        row_count: int = fact.count()
        logger.info(
            "[fact_carbon_intensity_hourly] OK — %d filas escritas en Gold.", row_count
        )
        return fact

    except Exception as exc:
        logger.error("[fact_carbon_intensity_hourly] ERROR: %s", exc, exc_info=True)
        raise


def build_fact_country_energy_annual() -> DataFrame:
    """
    Tabla de hechos de indicadores macroenergéticos y económicos anuales por país.

    Fuentes Silver:
      - owid/                  → indicadores energéticos, GHG, GDP, population
      - world_bank/ict_exports/ → exportaciones ICT por (country_code, year)

    Join:
      OWID (iso_code) ↔ World Bank (country_code) sobre iso_alpha3 + year.

    Surrogate Key de país:
      country_id → md5(iso_alpha3)  — mismo hash que dim_country.

    Partición física: year.
    Destino Gold: s3a://<gold>/fact_country_energy_annual/
    """
    logger.info("[fact_country_energy_annual] Iniciando construcción...")
    try:
        # ── 1. OWID ───────────────────────────────────────────────────────────
        owid: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/owid")
            .filter(F.col("iso_code").isNotNull() & (F.col("iso_code") != ""))
            .withColumn("iso_alpha3", F.upper(F.trim(F.col("iso_code"))))
            .select(
                F.col("iso_alpha3"),
                F.col("country").cast(StringType()).alias("country_name"),
                F.col("year").cast(IntegerType()),
                F.col("population").cast(DoubleType()),
                F.col("gdp").cast(DoubleType()),
                F.when(
                    F.col("gdp").isNotNull() & (F.col("population") > 0),
                    F.col("gdp") / F.col("population"),
                ).otherwise(F.lit(None).cast(DoubleType())).alias("gdp_per_capita"),
                F.col("carbon_intensity_elec").cast(DoubleType()),
                F.col("low_carbon_share_elec").cast(DoubleType()),
                F.col("renewables_share_elec").cast(DoubleType()),
                F.col("fossil_share_elec").cast(DoubleType()),
                F.col("electricity_demand").cast(DoubleType()).alias("electricity_demand_twh"),
                F.col("electricity_generation").cast(DoubleType()).alias("electricity_generation_twh"),
                F.col("energy_per_capita").cast(DoubleType()),
                F.col("greenhouse_gas_emissions").cast(DoubleType()).alias("greenhouse_gas_emissions_mtco2eq"),
            )
        )

        # ── 2. World Bank ICT Exports ─────────────────────────────────────────
        wb: DataFrame = (
            spark.read.format("delta")
            .load(f"{SILVER}/world_bank/ict_exports")
            .filter(F.col("country_code").isNotNull())
            .withColumn("iso_alpha3", F.upper(F.trim(F.col("country_code"))))
            .select(
                F.col("iso_alpha3"),
                F.col("year").cast(IntegerType()),
                F.col("ict_exports_usd").cast(DoubleType()),
            )
            .dropDuplicates(["iso_alpha3", "year"])
        )

        # ── 3. JOIN OWID ↔ World Bank por (iso_alpha3, year) ──────────────────
        fact: DataFrame = owid.join(wb, on=["iso_alpha3", "year"], how="left")

        # ── 4. Surrogate Key de país ──────────────────────────────────────────
        fact = fact.withColumn(
            "country_id",
            F.md5(F.lower(F.trim(F.col("iso_alpha3")))),
        )

        # ── 5. Selección y cast final ─────────────────────────────────────────
        fact = fact.select(
            F.col("country_id").cast(StringType()),
            F.col("iso_alpha3").cast(StringType()),
            F.col("country_name").cast(StringType()),
            F.col("year").cast(IntegerType()),
            F.col("population").cast(DoubleType()),
            F.col("gdp").cast(DoubleType()),
            F.col("gdp_per_capita").cast(DoubleType()),
            F.col("carbon_intensity_elec").cast(DoubleType()),
            F.col("low_carbon_share_elec").cast(DoubleType()),
            F.col("renewables_share_elec").cast(DoubleType()),
            F.col("fossil_share_elec").cast(DoubleType()),
            F.col("electricity_demand_twh").cast(DoubleType()),
            F.col("electricity_generation_twh").cast(DoubleType()),
            F.col("energy_per_capita").cast(DoubleType()),
            F.col("greenhouse_gas_emissions_mtco2eq").cast(DoubleType()),
            F.col("ict_exports_usd").cast(DoubleType()),
        )

        # ── 6. Escribir en Gold particionado por año ──────────────────────────
        (
            fact.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year")
            .save(f"{GOLD}/fact_country_energy_annual")
        )

        row_count: int = fact.count()
        logger.info(
            "[fact_country_energy_annual] OK — %d filas escritas en Gold.", row_count
        )
        return fact

    except Exception as exc:
        logger.error("[fact_country_energy_annual] ERROR: %s", exc, exc_info=True)
        raise


# =============================================================================
# ORQUESTACIÓN PRINCIPAL
# =============================================================================

def main() -> None:
    """
    Pipeline Gold completo: Dimensiones → Hechos.

    Orden de ejecución:
      1. Dimensiones (sin dependencias entre sí → pueden paralelizarse en Airflow).
      2. Hechos (dependen de que las dimensiones existan en Gold para los joins).

    Política de fallos:
      - Si alguna dimensión falla, se aborta antes de construir los hechos.
      - Si un hecho falla de forma aislada, los demás hechos continúan.
      - Al final se reporta el estado global y se sale con código 1 si hay errores.
    """
    logger.info("=" * 70)
    logger.info("  Green AI — Silver → Gold  |  Pipeline Dimensional Kimball")
    logger.info("=" * 70)

    # ── Paso 1: Dimensiones ───────────────────────────────────────────────────
    logger.info("--- PASO 1/2: Construcción de Dimensiones ---")
    dim_pipeline = [
        build_dim_country,
        build_dim_region,
        build_dim_gpu_model,
        build_dim_instance_type,
        build_dim_electricity_price,
    ]

    dim_errors: list[str] = []
    for fn in dim_pipeline:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("Dimensión fallida [%s]: %s", fn.__name__, exc)
            dim_errors.append(fn.__name__)

    if dim_errors:
        logger.error(
            "❌ %d dimensión(es) fallaron: %s. Abortando pipeline de hechos.",
            len(dim_errors),
            ", ".join(dim_errors),
        )
        spark.stop()
        sys.exit(1)

    logger.info("✅ Todas las dimensiones construidas correctamente.")

    # ── Paso 2: Hechos ────────────────────────────────────────────────────────
    logger.info("--- PASO 2/2: Construcción de Tablas de Hechos ---")
    fact_pipeline = [
        build_fact_ai_compute_usage,
        build_fact_carbon_intensity_hourly,
        build_fact_country_energy_annual,
    ]

    fact_errors: list[str] = []
    for fn in fact_pipeline:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("Hecho fallido [%s]: %s", fn.__name__, exc)
            fact_errors.append(fn.__name__)

    # ── Resumen final ─────────────────────────────────────────────────────────
    logger.info("=" * 70)
    if not fact_errors:
        logger.info("✅ Pipeline Gold completado exitosamente. Todas las tablas escritas.")
    else:
        logger.error(
            "❌ %d tabla(s) de hechos fallaron: %s",
            len(fact_errors),
            ", ".join(fact_errors),
        )

    spark.stop()
    logger.info("SparkSession cerrada.")

    if fact_errors:
        sys.exit(1)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    main()

