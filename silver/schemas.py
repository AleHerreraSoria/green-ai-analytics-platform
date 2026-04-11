"""
schemas.py
===========
Definición centralizada de los StructType de Spark para la capa Silver del
proyecto Green AI.

FUENTE DE VERDAD: DICCIONARIO_BRONZE_ACTUALIZADO.md  (auditoría empírica del
                 bucket S3  green-ai-pf-bronze-a0e96d06)

Convención de nombres:
  - Todos los esquemas se exportan como constantes en SCREAMING_SNAKE_CASE.
  - El sufijo _SCHEMA identifica que es un StructType de Spark.
  - Los campos ya aparecen en snake_case (versión Silver); la mapeo desde
    camelCase o espacios se realiza en bronze_to_silver.py.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# =============================================================================
# 1. ELECTRICITY MAPS — Intensidad de Carbono (Latest / Past)
# =============================================================================
# Ambos endpoints comparten estructura plana idéntica.
# Fuente: columnas de la muestra en DICCIONARIO_BRONZE_ACTUALIZADO (§ Latest/Past).

# NOTA: los nombres aquí son los ORIGINALES del JSON Bronze (camelCase).
# El renombrado a snake_case ocurre en bronze_to_silver.py via withColumnRenamed.
# Usar nombres Silver aquí causaría NULLs silenciosos en la lectura.
CARBON_INTENSITY_FLAT_SCHEMA = StructType([
    StructField("zone",                StringType(),    nullable=False),
    StructField("carbonIntensity",     IntegerType(),   nullable=True),
    StructField("datetime",            TimestampType(), nullable=False),
    StructField("updatedAt",           StringType(),    nullable=True),
    StructField("createdAt",           StringType(),    nullable=True),
    StructField("emissionFactorType",  StringType(),    nullable=True),
    StructField("isEstimated",         BooleanType(),   nullable=True),
    StructField("estimationMethod",    StringType(),    nullable=True),
    StructField("temporalGranularity", StringType(),    nullable=True),
    StructField("_disclaimer",         StringType(),    nullable=True),
])

# =============================================================================
# 2. ELECTRICITY MAPS — Intensidad de Carbono History
# =============================================================================
# El JSON de History contiene un array 'history' con registros idénticos al
# esquema plano de Latest. Se define el tipo del elemento del array para que
# el explode() posterior en bronze_to_silver.py sea schema-aware.

CARBON_INTENSITY_HISTORY_EVENT_SCHEMA = StructType([
    StructField("zone",                 StringType(),    nullable=True),
    StructField("carbonIntensity",      IntegerType(),   nullable=True),
    StructField("datetime",             StringType(),    nullable=True),  # ISO-8601 → cast a TS en Silver
    StructField("updatedAt",            StringType(),    nullable=True),
    StructField("createdAt",            StringType(),    nullable=True),
    StructField("emissionFactorType",   StringType(),    nullable=True),
    StructField("isEstimated",          BooleanType(),   nullable=True),
    StructField("estimationMethod",     StringType(),    nullable=True),
    StructField("temporalGranularity",  StringType(),    nullable=True),
])

# NOTA: nullable=True en zone e history porque en Hive-partitioned JSON el campo
# 'zone' puede estar sólo en el nombre del directorio (zone=AUS-NSW), no en el
# body del JSON. Con nullable=False Spark descartaría silenciosamente toda la fila.
CARBON_INTENSITY_HISTORY_RAW_SCHEMA = StructType([
    StructField("zone",                 StringType(),    nullable=True),
    StructField("history",              ArrayType(CARBON_INTENSITY_HISTORY_EVENT_SCHEMA), nullable=True),
    StructField("temporalGranularity",  StringType(),    nullable=True),
    StructField("_disclaimer",          StringType(),    nullable=True),
])

# =============================================================================
# 3. ELECTRICITY MAPS — Mix Eléctrico (Latest)
# =============================================================================
# El JSON contiene un array 'data' con registros por timestamp. Se definen
# los tipos del elemento del array para el explode() posterior.

ELECTRICITY_MIX_DATA_ROW_SCHEMA = StructType([
    StructField("datetime",          StringType(),  nullable=True),
    StructField("updatedAt",         StringType(),  nullable=True),
    StructField("createdAt",         StringType(),  nullable=True),
    StructField("nuclear",           DoubleType(),  nullable=True),
    StructField("geothermal",        DoubleType(),  nullable=True),
    StructField("biomass",           DoubleType(),  nullable=True),
    StructField("coal",              DoubleType(),  nullable=True),
    StructField("wind",              DoubleType(),  nullable=True),
    StructField("solar",             DoubleType(),  nullable=True),
    StructField("hydro",             DoubleType(),  nullable=True),
    StructField("gas",               DoubleType(),  nullable=True),
    StructField("oil",               DoubleType(),  nullable=True),
    StructField("unknown",           DoubleType(),  nullable=True),
    StructField("hydro_discharge",   DoubleType(),  nullable=True),
    StructField("battery_discharge", DoubleType(),  nullable=True),
])

# NOTA: nullable=True en zone y data por la misma razón que history:
# las particiones Hive separan zone en el path, no en el body del JSON.
ELECTRICITY_MIX_RAW_SCHEMA = StructType([
    StructField("zone",                StringType(),    nullable=True),
    StructField("temporalGranularity", StringType(),    nullable=True),
    StructField("unit",                StringType(),    nullable=True),
    StructField("data",                ArrayType(ELECTRICITY_MIX_DATA_ROW_SCHEMA), nullable=True),
    StructField("_disclaimer",         StringType(),    nullable=True),
])

# =============================================================================
# 4. ELECTRICITY MAPS — Catálogo de Zonas
# =============================================================================
# El JSON de zones es un mapa {zoneKey: {zoneName, countryName, countryCode}}.
# Se procesa aplanando en bronze_to_silver.py antes de materializar.

ZONES_CATALOG_SCHEMA = StructType([
    StructField("zone_key",      StringType(), nullable=False),
    StructField("zone_name",     StringType(), nullable=True),
    StructField("country_name",  StringType(), nullable=True),
    StructField("country_code",  StringType(), nullable=True),
])

# =============================================================================
# 5. GLOBAL PETROL PRICES
# =============================================================================
# Fuente: DICCIONARIO_BRONZE_ACTUALIZADO §global_petrol_prices
# Las columnas se renombran a snake_case en Silver.

GLOBAL_PETROL_PRICES_SCHEMA = StructType([
    StructField("country",                 StringType(),  nullable=False),
    StructField("residential_usd_per_kwh", DoubleType(),  nullable=True),
    StructField("business_usd_per_kwh",    DoubleType(),  nullable=True),
    # Campo derivado en Silver (ver bronze_to_silver.py):
    # precio_red_estimado_usd: imputación sintética por mes del log
])

# =============================================================================
# 6. MLCO2 — Promedios Anuales por Zona (yearly_averages)
# =============================================================================

# NOTA: los nombres coinciden con los encabezados EXACTOS del CSV Bronze.
# El renombrado a snake_case (Zone Name → zone_name, Country → country)
# ocurre en bronze_to_silver.py via withColumnRenamed.
MLCO2_YEARLY_AVG_SCHEMA = StructType([
    StructField("zone_key",              StringType(),  nullable=False),
    StructField("year",                  IntegerType(), nullable=False),
    StructField("Country",               StringType(),  nullable=True),
    StructField("Zone Name",             StringType(),  nullable=True),
    StructField("carbon_intensity_avg",  DoubleType(),  nullable=True),
    StructField("no_hours_with_data",    LongType(),    nullable=True),
])

# =============================================================================
# 7. OWID — Energy Data
# =============================================================================
# 130 columnas: se definen únicamente las relevantes para las preguntas de
# negocio. Las restantes se cargará como StringType y se descartarán en la
# selección Silver.

OWID_ENERGY_SCHEMA = StructType([
    StructField("country",                  StringType(),  nullable=False),
    StructField("year",                     IntegerType(), nullable=False),
    StructField("iso_code",                 StringType(),  nullable=True),
    StructField("population",               DoubleType(),  nullable=True),
    StructField("gdp",                      DoubleType(),  nullable=True),
    StructField("carbon_intensity_elec",    DoubleType(),  nullable=True),
    StructField("electricity_demand",       DoubleType(),  nullable=True),
    StructField("electricity_generation",   DoubleType(),  nullable=True),
    StructField("renewables_share_elec",    DoubleType(),  nullable=True),
    StructField("fossil_share_elec",        DoubleType(),  nullable=True),
    StructField("low_carbon_share_elec",    DoubleType(),  nullable=True),
    StructField("greenhouse_gas_emissions", DoubleType(),  nullable=True),
    StructField("solar_share_elec",         DoubleType(),  nullable=True),
    StructField("wind_share_elec",          DoubleType(),  nullable=True),
    StructField("hydro_share_elec",         DoubleType(),  nullable=True),
    StructField("nuclear_share_elec",       DoubleType(),  nullable=True),
    StructField("coal_share_elec",          DoubleType(),  nullable=True),
    StructField("gas_share_elec",           DoubleType(),  nullable=True),
    StructField("oil_share_elec",           DoubleType(),  nullable=True),
    StructField("primary_energy_consumption", DoubleType(), nullable=True),
    StructField("energy_per_capita",        DoubleType(),  nullable=True),
    StructField("energy_per_gdp",           DoubleType(),  nullable=True),
])

# =============================================================================
# 8. REFERENCE — Precios EC2 On-Demand
# =============================================================================

EC2_PRICING_SCHEMA = StructType([
    StructField("cloud_provider",      StringType(),  nullable=False),
    StructField("cloud_region",        StringType(),  nullable=False),
    StructField("instance_type",       StringType(),  nullable=False),
    StructField("operating_system",    StringType(),  nullable=True),
    StructField("tenancy",             StringType(),  nullable=True),
    StructField("pricing_model",       StringType(),  nullable=True),
    StructField("currency",            StringType(),  nullable=True),
    StructField("price_usd_per_hour",  DoubleType(),  nullable=False),
    StructField("price_basis_region",  StringType(),  nullable=True),
    StructField("regional_multiplier", DoubleType(),  nullable=True),
    StructField("as_of_date",          StringType(),  nullable=True),   # cast a Date en Silver
    StructField("source",              StringType(),  nullable=True),
    StructField("pricing_notes",       StringType(),  nullable=True),
])

# =============================================================================
# 9. USAGE LOGS (Sintético)
# =============================================================================

USAGE_LOGS_SCHEMA = StructType([
    StructField("session_id",          StringType(),  nullable=False),
    StructField("user_id",             StringType(),  nullable=True),
    StructField("timestamp",           StringType(),  nullable=True),  # cast a TS en Silver
    StructField("gpu_model",           StringType(),  nullable=True),
    StructField("region",              StringType(),  nullable=True),
    StructField("duration_hours",      DoubleType(),  nullable=True),
    StructField("instance_type",       StringType(),  nullable=True),
    StructField("gpu_utilization",     DoubleType(),  nullable=True),
    StructField("job_type",            StringType(),  nullable=True),
    StructField("energy_consumed_kwh", DoubleType(),  nullable=True),
    StructField("execution_status",    StringType(),  nullable=True),
])

# =============================================================================
# 10. WORLD BANK — ICT Service Exports
# =============================================================================
# CSV con metadatos en las primeras 4 filas → skiprows en lectura Bronze.
# Las columnas de año (1960–2025) se despivotan (melt) en Silver a formato
# (country_code, year, ict_exports_usd).

WORLD_BANK_ICT_SCHEMA = StructType([
    StructField("country_name",    StringType(),  nullable=True),
    StructField("country_code",    StringType(),  nullable=False),
    StructField("indicator_name",  StringType(),  nullable=True),
    StructField("indicator_code",  StringType(),  nullable=True),
    # Las columnas de año son dinámicas; se leen como string y se despivota.
])
