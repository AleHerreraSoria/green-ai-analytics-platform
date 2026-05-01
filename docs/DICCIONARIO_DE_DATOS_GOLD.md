# Diccionario de Datos Gold — Green AI Analytics Platform

> **Versión:** 1.0
> **Capa:** Gold
> **Ámbito:** Tablas de hechos y dimensiones finales del modelo analítico.

## 1. Resumen

Este documento describe la estructura técnica final de la capa Gold:

- Tablas de dimensiones
- Tablas de hechos
- Definición de columnas
- Lógica de cálculo / agregación
- Esquema de particionamiento final

La implementación se basa en el pipeline `spark/jobs/etl/silver_to_gold.py`.

## 2. Listado de tablas Gold

### Dimensiones

- `dim_country`
- `dim_region`
- `dim_gpu_model`
- `dim_instance_type`
- `dim_electricity_price`

### Hechos

- `fact_ai_compute_usage`
- `fact_carbon_intensity_hourly`
- `fact_country_energy_annual`

## 3. Esquema de particionamiento final en Gold

### Dimensiones

Las tablas de dimensión se escriben con `mode("overwrite")` total y no se particionan físicamente.

### Hechos

- `fact_ai_compute_usage` → `partitionBy("year", "month")`
- `fact_carbon_intensity_hourly` → `partitionBy("year", "month")`
- `fact_country_energy_annual` → `partitionBy("year")`

### Configuración Spark crítica para Gold

- `spark.sql.sources.partitionOverwriteMode = dynamic`

Esta configuración garantiza que la sobrescritura de particiones en las tablas de hechos sea dinámica. Esto significa que una ejecución del pipeline vuelve a escribir solo las particiones presentes en el lote actual, sin borrar otras particiones históricas.

## 4. Tablas de Dimensión

### 4.1 `dim_country`

Descripción: catálogo de países enriquecido con datos de OWID y metadatos de World Bank.

- `country_id` (STRING)
  - Clave surrogate determinística.
  - Lógica: `md5(lower(trim(iso_alpha3)))`.
- `country_name` (STRING)
  - Nombre del país desde OWID.
- `iso_alpha3` (STRING)
  - Código ISO Alpha-3 del país.
- `region` (STRING)
  - Región del World Bank asociada al país.
- `income_group` (STRING)
  - Grupo de ingresos según World Bank.

Fuentes Silver:
- `silver/owid`
- `silver/reference/world_bank_metadata`

### 4.2 `dim_region`

Descripción: tabla puente de regiones cloud que mapea región AWS a zona de Electricity Maps y país.

- `region_id` (STRING)
  - Clave surrogate determinística.
  - Lógica: `md5(lower(cloud_provider) || '|' || lower(cloud_region))`.
- `cloud_provider` (STRING)
  - Proveedor cloud (por ejemplo, `aws`).
- `aws_region_code` (STRING)
  - Código de región cloud (`cloud_region`).
- `aws_region_name` (STRING)
  - Nombre descriptivo de la región en MLCO2.
- `electricity_maps_zone` (STRING)
  - Nombre de la zona en Electricity Maps.
- `country_name` (STRING)
  - Nombre de país desde MLCO2 geo mapping.
- `country_id` (STRING)
  - FK a `dim_country` si existe `iso_alpha3`.
  - Lógica: `md5(lower(trim(iso_alpha3)))`.
- `iso_alpha2` (STRING)
  - Código ISO Alpha-2 del país.
- `iso_alpha3` (STRING)
  - Código ISO Alpha-3 del país.
- `is_primary_zone` (BOOLEAN)
  - Indicador si es la zona primaria para la región.
- `mapping_notes` (STRING)
  - Notas de mapeo / validación.

Fuentes Silver:
- `silver/reference/geo_cloud_mapping`

### 4.3 `dim_gpu_model`

Descripción: catálogo de GPUs MLCO2 con especificaciones clave para evaluar eficiencia energética.

- `gpu_id` (STRING)
  - Clave surrogate determinística.
  - Lógica: `md5(lower(trim(gpu_model)))`.
- `gpu_model` (STRING)
  - Nombre del modelo de GPU.
- `gpu_type` (STRING)
  - Tipo de GPU desde MLCO2.
- `tdp_watts` (INTEGER)
  - Potencia térmica de diseño en vatios.
- `gflops_fp32` (DOUBLE)
  - Rendimiento en TFLOPS FP32.
- `gflops_fp16` (DOUBLE)
  - Rendimiento en TFLOPS FP16.
- `memory_spec` (STRING)
  - Especificación de memoria de la GPU.

Fuentes Silver:
- `silver/mlco2/gpus`

### 4.4 `dim_instance_type`

Descripción: catálogo de tipos de instancia EC2 con precio por hora y metadatos de facturación.

- `instance_type_id` (STRING)
  - Clave surrogate determinística.
  - Lógica: `md5(lower(instance_type) || '|' || lower(cloud_region))`.
- `provider` (STRING)
  - Proveedor cloud del precio.
- `cloud_region` (STRING)
  - Región cloud asociada.
- `instance_type` (STRING)
  - Nombre del tipo de instancia EC2.
- `os_type` (STRING)
  - Sistema operativo del precio.
- `price_usd_per_hour` (DOUBLE)
  - Precio On-Demand en USD/hora.
- `pricing_model` (STRING)
  - Modelo de precio (por ejemplo, `on_demand`).
- `price_source_date` (STRING)
  - Fecha de origen del precio.
- `source` (STRING)
  - Fuente del precio.
- `notes` (STRING)
  - Notas adicionales de pricing.

Fuentes Silver:
- `silver/reference/ec2_pricing`

### 4.5 `dim_electricity_price`

Descripción: precio eléctrico residencial de referencia por país.

- `price_id` (STRING)
  - Clave surrogate determinística.
  - Lógica: `md5(lower(trim(country)))`.
- `country_name` (STRING)
  - Nombre del país en la fuente de precios.
- `year` (INTEGER)
  - Siempre `NULL` en la implementación actual.
- `month` (INTEGER)
  - Siempre `NULL` en la implementación actual.
- `price_usd_per_kwh` (DOUBLE)
  - Precio residencial en USD por kWh.
- `price_type` (STRING)
  - Tipo de precio (`residential`).
- `price_source` (STRING)
  - Fuente del precio (`global_petrol_prices`).
- `price_reference_date` (STRING)
  - Fecha de referencia del precio (NULL en implementación actual).

Fuentes Silver:
- `silver/global_petrol_prices`

## 5. Tablas de Hechos

### 5.1 `fact_ai_compute_usage`

Descripción: sesión de cómputo de IA registrada con métricas de energía, costo y emisiones.

#### Partición

- `year`, `month`

#### Columnas

- `session_id` (STRING)
  - Identificador único de la sesión.
- `user_id` (STRING)
  - Identificador del usuario.
- `timestamp` (STRING)
  - Marca de tiempo original del evento.
- `event_timestamp` (TIMESTAMP)
  - Timestamp parseado desde `timestamp`.
- `year` (INTEGER)
  - Año extraído de `timestamp`.
- `month` (INTEGER)
  - Mes extraído de `timestamp`.

#### FK y Surrogate Keys

- `region_id` (STRING)
  - `md5(lower(cloud_provider) || '|' || lower(region))`
  - FK a `dim_region`.
- `gpu_id` (STRING)
  - `md5(lower(trim(gpu_model)))`
  - FK a `dim_gpu_model`.
- `instance_type_id` (STRING)
  - `md5(lower(trim(instance_type)) || '|' || lower(trim(region)))`
  - FK a `dim_instance_type`.
- `price_id` (STRING)
  - `md5(lower(trim(country_name_geo)))`
  - FK a `dim_electricity_price`.

#### Dimensiones degeneradas

- `job_type` (STRING)
  - Valores esperados: `Training`, `Inference`, `Fine-tuning`.
- `execution_status` (STRING)
  - Valores esperados: `Success`, `Failed`.

#### Atributos de contexto y negocio

- `gpu_model` (STRING)
  - Modelo de GPU usado en la sesión.
- `instance_type` (STRING)
  - Tipo de instancia EC2 usado.
- `region` (STRING)
  - Región AWS original del log.
- `iso_alpha3` (STRING)
  - Código ISO Alpha-3 asociado vía `geo_cloud_mapping`.

#### Medidas y cálculos

- `duration_hours` (DOUBLE)
  - Duración de la sesión en horas.
- `gpu_utilization` (DOUBLE)
  - Porcentaje de utilización de la GPU.
- `energy_consumed_kwh` (DOUBLE)
  - Energía consumida en kWh.
- `price_kwh_usd` (DOUBLE)
  - Precio residencial por kWh obtenido desde `global_petrol_prices`.
- `price_compute_usd_per_hour` (DOUBLE)
  - Precio de cómputo por hora desde `ec2_pricing`.
- `cost_electricity_usd` (DOUBLE)
  - Lógica: `energy_consumed_kwh * residential_usd_per_kwh`.
- `cost_compute_usd` (DOUBLE)
  - Lógica: `duration_hours * price_usd_per_hour`.

#### Fuentes y joins

- `silver/usage_logs`
- `silver/reference/geo_cloud_mapping`
- `silver/reference/ec2_pricing`
- `silver/global_petrol_prices`

#### Notas de cálculo

- El join con `global_petrol_prices` se realiza por país normalizado desde `country_name_geo`.
- Si algún factor es `NULL`, la métrica calculada también resulta `NULL`.

### 5.2 `fact_carbon_intensity_hourly`

Descripción: intensidad de carbono horaria por zona eléctrica.

#### Partición

- `year`, `month`

#### Columnas

- `zone` (STRING)
  - Nombre de la zona en Electricity Maps.
- `region_id` (STRING)
  - `md5(lower(cloud_provider) || '|' || lower(cloud_region))`
  - FK a `dim_region`.
- `iso_alpha3` (STRING)
  - Código ISO Alpha-3 de país asociado.
- `event_ts` (TIMESTAMP)
  - Timestamp parseado del campo `datetime`.
- `year` (INTEGER)
  - Año del evento.
- `month` (INTEGER)
  - Mes del evento.
- `hour` (INTEGER)
  - Hora del evento.
- `emission_factor_type` (STRING)
  - Tipo de factor de emisión (`lifecycle`, etc.).
- `is_estimated` (BOOLEAN)
  - Indicador si el valor es estimado.
- `carbon_intensity_gco2eq_per_kwh` (DOUBLE)
  - Intensidad de carbono en gCO₂e/kWh.

#### Fuentes y joins

- `silver/electricity_maps/carbon_intensity/history/`
- `silver/electricity_maps/carbon_intensity/latest/`
- `silver/reference/geo_cloud_mapping`

#### Notas de construcción

- El pipeline intenta leer la tabla `history` primero; si no está disponible, lee `latest`; si tampoco, lee `past`.
- El `region_id` se resuelve por zona eléctrica (`zone`) usando el mapeo de `geo_cloud_mapping`.

### 5.3 `fact_country_energy_annual`

Descripción: indicadores macroenergéticos y económicos anuales por país.

#### Partición

- `year`

#### Columnas

- `country_id` (STRING)
  - Clave surrogate de país.
  - Lógica: `md5(lower(trim(iso_alpha3)))`.
- `iso_alpha3` (STRING)
  - Código ISO Alpha-3 del país.
- `country_name` (STRING)
  - Nombre descriptivo del país.
- `year` (INTEGER)
  - Año del indicador.
- `population` (DOUBLE)
  - Población total.
- `gdp` (DOUBLE)
  - Producto Interno Bruto total.
- `gdp_per_capita` (DOUBLE)
  - Lógica: `gdp / population` si población > 0.
- `carbon_intensity_elec` (DOUBLE)
  - Intensidad de carbono de la electricidad.
- `low_carbon_share_elec` (DOUBLE)
  - Porcentaje de electricidad de baja emisión.
- `renewables_share_elec` (DOUBLE)
  - Porcentaje de generación eléctrica renovable.
- `fossil_share_elec` (DOUBLE)
  - Porcentaje de generación eléctrica fósil.
- `electricity_demand_twh` (DOUBLE)
  - Demanda eléctrica en TWh.
- `electricity_generation_twh` (DOUBLE)
  - Generación eléctrica en TWh.
- `energy_per_capita` (DOUBLE)
  - Energía per cápita.
- `greenhouse_gas_emissions_mtco2eq` (DOUBLE)
  - Emisiones de gases de efecto invernadero en Mt CO₂e.
- `ict_exports_usd` (DOUBLE)
  - Exportaciones de TIC en USD.

#### Fuentes y joins

- `silver/owid`
- `silver/world_bank/ict_exports`

#### Notas de construcción

- El join se realiza sobre `(iso_alpha3, year)`.
- Se filtran datos solo dentro del rango `1990 <= year <= 2030`.

## 6. Observaciones finales

- El diccionario está basado en la implementación actual del pipeline Gold.
- Las tablas y columnas descritas reflejan la lógica concreta que se ejecuta en `spark/jobs/etl/silver_to_gold.py`.
- Si se desea un nivel adicional de detalle, se puede ampliar con ejemplos de consultas, estándares de nombres y medidas de calidad específicas.
