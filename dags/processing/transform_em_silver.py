from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
import sys
import pendulum

def transform_bronze_to_silver(bronze_path, silver_path):
    # 1. Iniciar Sesión de Spark con soporte para S3
    spark = SparkSession.builder \
        .appName("EM_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print(f"Reading data from: {bronze_path}")

    # 2. Definir el esquema del JSON basado en la documentación
    # Esto es "Ultra PRO" porque evita que Spark tenga que inferir el esquema (lento)
    data_schema = StructType([
        StructField("carbonIntensity", IntegerType(), True),
        StructField("datetime", StringType(), True),
        StructField("isEstimated", BooleanType(), True),
        StructField("estimationMethod", StringType(), True)
    ])

    base_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("data", ArrayType(data_schema), True)
    ])

    # 3. Leer los archivos JSON de Bronze
    df_raw = spark.read.option("multiline", "true").schema(base_schema).json(bronze_path)

    # 4. Transformación: "Explode" y Aplanado
    # El JSON original tiene una lista 'data'. Queremos una fila por cada elemento de esa lista.
    df_silver = df_raw.withColumn("record", explode(col("data"))) \
        .select(
            col("zone"),
            to_timestamp(col("record.datetime")).alias("datetime_utc"),
            col("record.carbonIntensity").alias("carbon_intensity_g_co2_kwh"),
            col("record.isEstimated").alias("is_estimated"),
            col("record.estimationMethod").alias("estimation_method"),
            lit(pendulum.now('UTC').to_date_string()).alias("ingestion_date") # Auditoría
        )

    # 5. Limpieza Final: Eliminar duplicados si existen
    df_silver = df_silver.dropDuplicates(["zone", "datetime_utc"])

    # 6. Escritura en Silver (Parquet particionado por zona)
    print(f"Writing data to: {silver_path}")
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("zone") \
        .parquet(silver_path)

    spark.stop()

if __name__ == "__main__":
    # Estos argumentos los pasaremos luego desde el DAG de Airflow
    BRONZE_INPUT = sys.argv[1]
    SILVER_OUTPUT = sys.argv[2]
    transform_bronze_to_silver(BRONZE_INPUT, SILVER_OUTPUT)