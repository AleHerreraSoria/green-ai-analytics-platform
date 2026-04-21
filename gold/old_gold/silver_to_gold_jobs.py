from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, coalesce, lit, when

spark = SparkSession.builder \
    .appName("gold_fact_ai_jobs") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# -------------------------
# LOAD
# -------------------------
logs = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/usage_logs/")

geo = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/reference/geo_cloud_mapping/") \
    .select("cloud_region", "country_name_mlco2")

mlco2 = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/mlco2/yearly_averages/") \
    .select("country", "carbon_intensity_avg")

prices = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/global_petrol_prices/") \
    .select("country", "residential_usd_per_kwh")

ec2 = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/reference/ec2_pricing/") \
    .select("cloud_region", "instance_type", "price_usd_per_hour")

owid = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/owid/")
wb = spark.read.parquet("s3a://green-ai-pf-silver-a0e96d06/world_bank/ict_exports/")

# -------------------------
# NORMALIZE
# -------------------------
logs = logs.withColumn("region_clean", lower(trim(col("region"))))

geo = geo.withColumn("region_clean", lower(trim(col("cloud_region")))) \
         .withColumn("country_mlco2_clean", lower(trim(col("country_name_mlco2")))) \
         .drop("cloud_region")

mlco2 = mlco2.withColumn("country_clean", lower(trim(col("country")))) \
             .drop("country") \
             .dropDuplicates(["country_clean"])

prices = prices.withColumn("country_clean", lower(trim(col("country")))) \
               .drop("country") \
               .dropDuplicates(["country_clean"])

ec2 = ec2.withColumn("region_clean", lower(trim(col("cloud_region")))) \
         .withColumn("instance_clean", lower(trim(col("instance_type")))) \
         .drop("cloud_region", "instance_type") \
         .dropDuplicates(["region_clean", "instance_clean"])

# -------------------------
# JOINS BASE
# -------------------------
df = logs.join(geo, "region_clean", "left")

df = df.join(mlco2, df["country_mlco2_clean"] == mlco2["country_clean"], "left") \
       .drop("country_clean")

df = df.join(
    prices,
    lower(trim(df["country_name_mlco2"])) == prices["country_clean"],
    "left"
)

df = df.join(ec2,
             (df["region_clean"] == ec2["region_clean"]) &
             (lower(trim(df["instance_type"])) == ec2["instance_clean"]),
             "left") \
       .drop(ec2["region_clean"]).drop("instance_clean")

# -------------------------
# COUNTRY CLEAN
# -------------------------
df = df.withColumn("country_clean", lower(trim(col("country_name_mlco2"))))

df = df.withColumn("country_clean",
    when(col("country_clean") == "usa", "united states")
    .when(col("country_clean") == "south korea", "korea, rep.")
    .otherwise(col("country_clean"))
)

# -------------------------
# OWID (2020)
# -------------------------
owid = owid.filter(col("year") == 2020)

owid = owid.withColumn("country_clean", lower(trim(col("country"))))

owid = owid.select(
    "country_clean",
    "gdp",
    "population",
    "carbon_intensity_elec"
)

owid = owid.withColumn(
    "gdp_per_capita",
    col("gdp") / col("population")
)

# -------------------------
# WORLD BANK (2020)
# -------------------------
wb = wb.filter(col("year") == 2020)

wb = wb.withColumn("country_clean", lower(trim(col("country_name"))))

wb = wb.select(
    "country_clean",
    col("ict_exports_usd").alias("ict_exports")
)

# -------------------------
# JOINS FINALES
# -------------------------
df = df.join(owid, "country_clean", "left")
df = df.join(wb, "country_clean", "left")

# -------------------------
# MÉTRICAS
# -------------------------
df = df.withColumn("energy_fixed", coalesce(col("energy_consumed_kwh"), lit(1.0)))
df = df.withColumn("duration_fixed", coalesce(col("duration_hours"), lit(1.0)))
df = df.withColumn("carbon_fixed", coalesce(col("carbon_intensity_avg"), lit(100.0)))

df = df.withColumn("price_kwh", coalesce(col("residential_usd_per_kwh"), lit(0.15)))
df = df.withColumn("price_compute", coalesce(col("price_usd_per_hour"), lit(1.0)))

df = df.withColumn("cost_energy", col("energy_fixed") * col("price_kwh"))
df = df.withColumn("cost_compute", col("duration_fixed") * col("price_compute"))
df = df.withColumn("emissions_co2", col("energy_fixed") * col("carbon_fixed"))

# -------------------------
# 🔥 FIX CLAVE: CAST TIPOS
# -------------------------
df = df.select(
    col("session_id").cast("string"),
    col("user_id").cast("string"),
    col("timestamp").cast("string"),
    col("gpu_model").cast("string"),
    col("region").cast("string"),
    col("duration_hours").cast("double"),
    col("instance_type").cast("string"),
    col("gpu_utilization").cast("double"),
    col("job_type").cast("string"),
    col("energy_consumed_kwh").cast("double"),
    col("execution_status").cast("string"),
    col("country_name_mlco2").cast("string"),
    col("carbon_intensity_avg").cast("double"),
    col("residential_usd_per_kwh").cast("double"),
    col("price_usd_per_hour").cast("double"),
    col("cost_energy").cast("double"),
    col("cost_compute").cast("double"),
    col("emissions_co2").cast("double"),
    col("country_clean").cast("string"),
    col("gdp").cast("double"),
    col("population").cast("double"),
    col("carbon_intensity_elec").cast("double"),
    col("gdp_per_capita").cast("double"),
    col("ict_exports").cast("double")
)

print("FILAS:", df.count())

# -------------------------
# WRITE FINAL (SIN repartition)
# -------------------------
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("s3a://green-ai-pf-gold-a0e96d06/fact_ai_jobs/")

print("✅ GOLD FINAL OK")