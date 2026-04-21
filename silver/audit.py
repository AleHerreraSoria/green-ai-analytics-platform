"""
audit.py
=========
Módulo de auditoría Bronze → Silver para el proyecto Green AI.

Responsabilidades:
  - Contar registros en cada dataset de la capa Bronze (archivos crudos).
  - Contar registros en cada dataset de la capa Silver (Parquet transformado).
  - Comparar ambos conteos y calcular la tasa de retención.
  - Detectar pérdidas de datos inesperadas (por encima de un umbral).
  - Persistir el resultado como un log de auditoría Parquet en S3
    (s3://SILVER/audit/audit_log/) particionado por fecha de ejecución.
  - Imprimir un reporte en consola que pueda integrarse con Airflow/Step Func.

Nota sobre conteos de Bronze:
  - Para CSVs: se usa spark.read.csv con header=true.
  - Para JSONs particionados (Electricity Maps): se usa recursiveFileLookup.
  - Para World Bank: se aplica skiprows=4 (ver bronze_to_silver.py).
  - La columna history de Carbon Intensity History se expande con explode;
    el conteo Bronze mide registros del array, no archivos.

Umbral de pérdida aceptable: MAX_LOSS_PCT (defecto 5%).
La diferencia entre Bronze y Silver puede deberse a:
  - Filtros de calidad (energy_consumed_kwh > 0 en usage_logs).
  - Transformaciones que expanden arrays → más filas en Silver.
  - Eliminación de metadatos de World Bank (skiprows).
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("green-ai.audit")

# ── Dependencias opcionales de desarrollo ──────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Entorno de producción: Airflow / IAM Role inyecta las vars

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

BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "green-ai-pf-bronze-a0e96d06")
SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "green-ai-pf-silver-a0e96d06")
BRONZE = f"s3a://{BRONZE_BUCKET}"
SILVER = f"s3a://{SILVER_BUCKET}"

# Umbral máximo de pérdida de registros Bronze → Silver aceptable (5%).
# Pérdidas mayores activan el estado WARN_HIGH_LOSS en AuditEntry.status.
MAX_LOSS_PCT = 0.05


# ---------------------------------------------------------------------------
# Estructura de resultado por dataset
# ---------------------------------------------------------------------------

@dataclass
class AuditEntry:
    dataset: str
    bronze_count: int
    silver_count: int
    run_timestamp: datetime

    @property
    def delta(self) -> int:
        return self.silver_count - self.bronze_count

    @property
    def retention_pct(self) -> float:
        if self.bronze_count == 0:
            return 0.0
        return self.silver_count / self.bronze_count

    @property
    def status(self) -> str:
        if self.bronze_count == 0:
            return "WARN_EMPTY_BRONZE"
        loss = (self.bronze_count - self.silver_count) / self.bronze_count
        if loss > MAX_LOSS_PCT:
            return "WARN_HIGH_LOSS"
        if self.silver_count > self.bronze_count:
            return "INFO_EXPANDED"   # explode genera más filas
        return "OK"


# ---------------------------------------------------------------------------
# Conteos Bronze
# ---------------------------------------------------------------------------

def _count_csv(spark, path: str, skip_rows: int = 0) -> int:
    try:
        if skip_rows > 0:
            import boto3, io, pandas as pd
            bucket, key = path.replace("s3a://", "").replace("s3://", "").split("/", 1)
            s3 = boto3.client("s3")
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            pdf = pd.read_csv(io.BytesIO(body), skiprows=skip_rows, dtype=str)
            return len(pdf)
        else:
            return spark.read.option("header", "true").csv(path).count()
    except Exception as e:
        print(f"    ⚠️  No se pudo contar Bronze CSV {path}: {e}")
        return -1


def _count_json_recursive(spark, path: str, explode_col: str = None) -> int:
    try:
        df = spark.read.option("recursiveFileLookup", "true").json(path)
        if explode_col and explode_col in df.columns:
            df = df.withColumn("_item", F.explode(explode_col))
        return df.count()
    except Exception as e:
        print(f"    ⚠️  No se pudo contar Bronze JSON {path}: {e}")
        return -1


def _count_silver(spark, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception as e:
        print(f"    ⚠️  No se pudo contar Silver {path}: {e}")
        return -1


def _count_observed_zones_bronze(spark) -> int:
    """Cuenta zonas únicas observadas desde endpoints activos."""
    try:
        ci_latest = (
            spark.read.option("recursiveFileLookup", "true")
            .json(f"{BRONZE}/electricity_maps/carbon_intensity/latest")
            .select(F.col("zone").alias("zone_key"))
        )
        ci_past = (
            spark.read.option("recursiveFileLookup", "true")
            .json(f"{BRONZE}/electricity_maps/carbon_intensity/past")
            .select(F.col("zone").alias("zone_key"))
        )
        ci_history = (
            spark.read.option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .json(f"{BRONZE}/electricity_maps/carbon_intensity/history")
            .withColumn("event", F.explode("history"))
            .select(F.col("event.zone").alias("zone_key"))
        )
        mix_latest = (
            spark.read.option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .json(f"{BRONZE}/electricity_maps/electricity_mix/latest")
            .select(F.col("zone").alias("zone_key"))
        )
        return (
            ci_latest.unionByName(ci_past)
            .unionByName(ci_history)
            .unionByName(mix_latest)
            .filter(F.col("zone_key").isNotNull())
            .distinct()
            .count()
        )
    except Exception as e:
        print(f"    ⚠️  No se pudo contar zonas observadas Bronze: {e}")
        return -1


# ---------------------------------------------------------------------------
# Función de auditoría principal
# ---------------------------------------------------------------------------

def run_audit(spark: SparkSession) -> list[AuditEntry]:
    now = datetime.now(timezone.utc)
    entries = []

    # Mapa: dataset_key → (bronze_count_fn, silver_path)
    AUDIT_PLAN = {
        "usage_logs": (
            lambda: _count_csv(spark, f"{BRONZE}/usage_logs/usage_logs.csv"),
            f"{SILVER}/usage_logs",
        ),
        "global_petrol_prices": (
            lambda: _count_csv(spark, f"{BRONZE}/global_petrol_prices/electricity_prices_by_country_2023_2026_avg.csv"),
            f"{SILVER}/global_petrol_prices",
        ),
        "mlco2/yearly_averages": (
            lambda: _count_csv(spark, f"{BRONZE}/mlco2/2021-10-27yearly_averages.csv"),
            f"{SILVER}/mlco2/yearly_averages",
        ),
        "mlco2/instances": (lambda: _count_csv(spark, f"{BRONZE}/mlco2/instances.csv"), f"{SILVER}/mlco2/instances"),
        "mlco2/impact": (lambda: _count_csv(spark, f"{BRONZE}/mlco2/impact.csv"), f"{SILVER}/mlco2/impact"),
        "mlco2/gpus": (lambda: _count_csv(spark, f"{BRONZE}/mlco2/gpus.csv"), f"{SILVER}/mlco2/gpus"),
        "owid": (
            lambda: _count_csv(spark, f"{BRONZE}/owid/owid-energy-data.csv"),
            f"{SILVER}/owid",
        ),
        "reference/ec2_pricing": (
            lambda: _count_csv(spark, f"{BRONZE}/reference/aws_ec2_on_demand_usd_per_hour.csv"),
            f"{SILVER}/reference/ec2_pricing",
        ),
        "reference/geo_cloud_mapping": (
            lambda: _count_csv(spark, f"{BRONZE}/reference/geo_cloud_to_country_and_zones.csv"),
            f"{SILVER}/reference/geo_cloud_mapping",
        ),
        # World Bank: 4 filas son metadatos → skip.
        # ⚠️  NOTA DE DISEÑO: Bronze count ≈ 266 filas (1 por país en formato ancho).
        # Silver count ≈ 266 × ~66 años = ~17,500 filas (formato largo tras melt).
        # El status INFO_EXPANDED es ESPERADO y correcto. No es una anomalía.
        "world_bank/ict_exports": (
            lambda: _count_csv(spark,
                               f"{BRONZE}/world_bank/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv",
                               skip_rows=4),
            f"{SILVER}/world_bank/ict_exports",
        ),
        "reference/world_bank_metadata": (
            lambda: _count_csv(spark, f"{BRONZE}/world_bank/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv"),
            f"{SILVER}/reference/world_bank_metadata",
        ),
        # Electricity Maps JSON planos
        "electricity_maps/carbon_intensity/latest": (
            lambda: _count_json_recursive(spark,
                                          f"{BRONZE}/electricity_maps/carbon_intensity/latest"),
            f"{SILVER}/electricity_maps/carbon_intensity/latest",
        ),
        "electricity_maps/carbon_intensity/past": (
            lambda: _count_json_recursive(spark,
                                          f"{BRONZE}/electricity_maps/carbon_intensity/past"),
            f"{SILVER}/electricity_maps/carbon_intensity/past",
        ),
        # History: se expande el array → Silver tendrá más filas que Bronze archivos
        "electricity_maps/carbon_intensity/history": (
            lambda: _count_json_recursive(spark,
                                          f"{BRONZE}/electricity_maps/carbon_intensity/history",
                                          explode_col="history"),
            f"{SILVER}/electricity_maps/carbon_intensity/history",
        ),
        "electricity_maps/electricity_mix/latest": (
            lambda: _count_json_recursive(spark,
                                          f"{BRONZE}/electricity_maps/electricity_mix/latest",
                                          explode_col="data"),
            f"{SILVER}/electricity_maps/electricity_mix/latest",
        ),
        "reference/zones_dimension": (
            lambda: _count_observed_zones_bronze(spark),
            f"{SILVER}/reference/zones_dimension",
        ),
    }

    for dataset, (bronze_fn, silver_path) in AUDIT_PLAN.items():
        print(f"\n[audit] {dataset}")
        bronze_count = bronze_fn()
        silver_count = _count_silver(spark, silver_path)
        entry = AuditEntry(
            dataset=dataset,
            bronze_count=bronze_count,
            silver_count=silver_count,
            run_timestamp=now,
        )
        entries.append(entry)
        print(f"    Bronze:    {bronze_count:>12,}")
        print(f"    Silver:    {silver_count:>12,}")
        print(f"    Delta:     {entry.delta:>+12,}")
        print(f"    Retención: {entry.retention_pct:>11.1%}")
        print(f"    Status:    {entry.status}")

    return entries


# ---------------------------------------------------------------------------
# Persistir log de auditoría en Silver
# ---------------------------------------------------------------------------

AUDIT_LOG_SCHEMA = StructType([
    StructField("dataset",         StringType(),    nullable=False),
    StructField("bronze_count",    IntegerType(),   nullable=True),
    StructField("silver_count",    IntegerType(),   nullable=True),
    StructField("delta",           IntegerType(),   nullable=True),
    StructField("retention_pct",   DoubleType(),    nullable=True),
    StructField("status",          StringType(),    nullable=True),
    StructField("run_timestamp",   TimestampType(), nullable=False),
])


def persist_audit_log(spark: SparkSession, entries: list[AuditEntry]):
    rows = [
        (
            e.dataset,
            e.bronze_count,
            e.silver_count,
            e.delta,
            round(e.retention_pct, 4),
            e.status,
            e.run_timestamp,
        )
        for e in entries
    ]

    df_log = spark.createDataFrame(rows, schema=AUDIT_LOG_SCHEMA)
    df_log = df_log.withColumn("run_date", F.to_date("run_timestamp"))

    run_date_str = df_log.select("run_date").first()[0].strftime("%Y-%m-%d")

    log_path = f"{SILVER}/audit/audit_log"
    (df_log.write
        .mode("overwrite")
        .format("delta")
        .option("replaceWhere", f"run_date = '{run_date_str}'")
        .save(log_path))
    print(f"\n[audit] Log persistido en: {log_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("green-ai-audit-bronze-silver")
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

    print("\n" + "=" * 60)
    print("  AUDITORÍA Bronze → Silver — Green AI")
    print("=" * 60)

    entries = run_audit(spark)
    persist_audit_log(spark, entries)

    # Reporte final
    print("\n" + "=" * 60)
    print("  RESUMEN")
    print("=" * 60)
    all_ok = all(e.status in ("OK", "INFO_EXPANDED") for e in entries)
    warns = [e for e in entries if "WARN" in e.status]
    if warns:
        print(f"  ⚠️  {len(warns)} dataset(s) con advertencias:")
        for e in warns:
            print(f"    - {e.dataset}: {e.status}")
    else:
        print("  ✅ Todos los datasets dentro del umbral de retención.")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
