# DOCS SILVER LAYER — Green AI Project

> **Versión:** 1.0  
> **Fecha:** 2026-04-09  
> **Responsable:** Data Engineering Team  
> **Bucket Bronze:** `green-ai-pf-bronze-a0e96d06`  
> **Bucket Silver:** `green-ai-pf-silver-a0e96d06`

---

## 1. Contexto y Objetivo

La capa Silver centraliza los datos crudos de la capa Bronze en un formato
analíticamente consumible: Parquet con compresión Snappy, esquemas
explícitamente definidos, nombres de columna en `snake_case` y fechas
casteadas a `TimestampType`. Cada dataset Silver es el insumo directo para
la capa Gold (KPIs, visualizaciones y modelos de correlación).

---

## 2. Estructura de Ramas (Simulación GitHub Flow)

| Carpeta / Rama                    | Responsabilidad                                      |
|-----------------------------------|------------------------------------------------------|
| `feature/schema-definition-spark` | Definición centralizada de StructTypes Spark         |
| `feature/spark-bronze-to-silver`  | Job ETL principal Bronze → Silver                    |
| `feature/quality-silver-validation` | Módulo de validación de calidad (CA Jira)           |
| `feature/storage-partitioning-silver` | Módulo de escritura con estrategia de partición  |
| `feature/audit-bronze-silver`     | Comparación de conteos Bronze vs Silver              |
| `feature/docs-silver-layer`       | Esta documentación técnica                           |

---

## 3. Schemas Silver por Dataset

### 3.1 `electricity_maps/carbon_intensity/{latest,past}` — Plano

Estructura plana (JSON ya resuelto). Escritura particionada por `zone/year/month`.

| Columna Silver        | Tipo         | Origen Bronze          | Notas                            |
|-----------------------|--------------|------------------------|----------------------------------|
| `zone`                | String       | `zone`                 | Clave de zona eléctrica          |
| `carbon_intensity`    | Integer      | `carbonIntensity`      | gCO₂eq/kWh; rango válido 0–1000 |
| `datetime`            | Timestamp    | `datetime`             | UTC                              |
| `updated_at`          | String       | `updatedAt`            |                                  |
| `created_at`          | String       | `createdAt`            |                                  |
| `emission_factor_type`| String       | `emissionFactorType`   | Ej: `lifecycle`                  |
| `is_estimated`        | Boolean      | `isEstimated`          |                                  |
| `estimation_method`   | String       | `estimationMethod`     |                                  |
| `temporal_granularity`| String       | `temporalGranularity`  | Ej: `hourly`                     |
| `year`                | Integer      | derivado de `datetime` | Columna de partición             |
| `month`               | Integer      | derivado de `datetime` | Columna de partición             |

> **Columna eliminada:** `_disclaimer` (marca de sandbox, no apta para producción).

---

### 3.2 `electricity_maps/carbon_intensity/history` — Explode de array

El JSON Bronze contiene un campo `history: [...]`. Se aplica `explode()` para
generar una fila por registro temporal. El resultado tiene el mismo esquema
Silver que `latest/past` pero puede tener **más filas** que archivos Bronze.

---

### 3.3 `electricity_maps/electricity_mix/latest` — Explode de array `data`

| Columna Silver        | Tipo      | Notas                                      |
|-----------------------|-----------|--------------------------------------------|
| `zone`                | String    |                                            |
| `unit`                | String    | `MW` para todas las fuentes de generación  |
| `temporal_granularity`| String    |                                            |
| `datetime`            | Timestamp | Extraído del array `data`                  |
| `nuclear_mw`          | Double    | Generación nuclear en MW                   |
| `geothermal_mw`       | Double    |                                            |
| `biomass_mw`          | Double    |                                            |
| `coal_mw`             | Double    |                                            |
| `wind_mw`             | Double    |                                            |
| `solar_mw`            | Double    |                                            |
| `hydro_mw`            | Double    |                                            |
| `gas_mw`              | Double    |                                            |
| `oil_mw`              | Double    |                                            |
| `unknown_mw`          | Double    |                                            |
| `hydro_discharge_mw`  | Double    |                                            |
| `battery_discharge_mw`| Double    |                                            |
| `year` / `month`      | Integer   | Columnas de partición derivadas            |

---

### 3.4 `electricity_maps/zones/catalog` — Mapa aplanado

| Columna Silver  | Tipo   | Origen Bronze                    |
|-----------------|--------|----------------------------------|
| `zone_key`      | String | Clave del mapa JSON (ej. `DE`)   |
| `zone_name`     | String | `zoneName`                       |
| `country_name`  | String | `countryName`                    |
| `country_code`  | String | `countryCode` (2 letras)         |

---

### 3.5 `global_petrol_prices` — Precios estáticos + columna derivada

| Columna Silver               | Tipo    | Origen Bronze                         | Notas                      |
|------------------------------|---------|---------------------------------------|----------------------------|
| `country`                    | String  | `Country`                             |                            |
| `residential_usd_per_kwh`    | Double  | columna larga renombrada              |                            |
| `business_usd_per_kwh`       | Double  | columna larga renombrada              | Nulos en 11 países         |
| **`precio_red_estimado_usd`**| Double  | **Imputación sintética** (ver §4)     | ⚠️ Ver nota crítica abajo  |

---

### 3.6 `mlco2/yearly_averages`

| Columna Silver        | Tipo    | Origen Bronze           |
|-----------------------|---------|-------------------------|
| `zone_key`            | String  | `zone_key`              |
| `year`                | Integer | `year`                  |
| `country`             | String  | `Country`               |
| `zone_name`           | String  | `Zone Name`             |
| `carbon_intensity_avg`| Double  | `carbon_intensity_avg`  |
| `no_hours_with_data`  | Long    | `no_hours_with_data`    |

---

### 3.7 `owid` — Selección de 22 columnas analíticas

OWID tiene 130 columnas. Silver retiene solo las 22 relevantes para las
preguntas de negocio (ver `schemas.py` sección 7 para lista completa).
Particionado por `year`.

---

### 3.8 `reference/ec2_pricing`

| Columna Silver        | Tipo   | Notas                              |
|-----------------------|--------|------------------------------------|
| `cloud_provider`      | String | Clave de partición                 |
| `cloud_region`        | String |                                    |
| `instance_type`       | String |                                    |
| `price_usd_per_hour`  | Double | Tarifa On-Demand (proxy TCO)       |
| `regional_multiplier` | Double | Factor aplicado sobre precio base  |
| `as_of_date`          | Date   | Cast desde `String` ISO            |
| *(más 7 columnas)*    |        | Ver `EC2_PRICING_SCHEMA` en schemas.py |

---

### 3.9 `usage_logs` — Logs de sesiones de IA (sintético)

| Columna Silver        | Tipo      | Notas                                           |
|-----------------------|-----------|-------------------------------------------------|
| `session_id`          | String    | UUID único por sesión; validado sin duplicados  |
| `user_id`             | String    |                                                 |
| `timestamp`           | Timestamp | Cast desde `"yyyy-MM-dd HH:mm:ss"`              |
| `gpu_model`           | String    |                                                 |
| `region`              | String    | Código de región AWS (ej. `ap-northeast-3`)     |
| `duration_hours`      | Double    |                                                 |
| `instance_type`       | String    |                                                 |
| `gpu_utilization`     | Double    | Rango [0, 1]                                    |
| `job_type`            | String    | Dominio: Training, Inference, Fine-tuning        |
| `energy_consumed_kwh` | Double    | Filtro de calidad: solo > 0                     |
| `execution_status`    | String    | Dominio: Success, Failed                        |
| `year` / `month`      | Integer   | Columnas de partición derivadas                 |

---

### 3.10 `world_bank/ict_exports` — Formato largo (melt)

El CSV Bronze tiene las exportaciones TIC en formato ancho (1 columna por año
1960–2025). Silver lo transforma a formato largo:

| Columna Silver      | Tipo    | Notas                              |
|---------------------|---------|------------------------------------|
| `country_name`      | String  |                                    |
| `country_code`      | String  | ISO-3 (clave de join con OWID)     |
| `indicator_name`    | String  |                                    |
| `indicator_code`    | String  | `BX.GSR.CCIS.CD`                   |
| `year`              | Integer | Columna de partición               |
| `ict_exports_usd`   | Double  | USD corrientes; nulos eliminados   |

> **Regla especial aplicada:** las primeras 4 filas del CSV (metadatos del
> extracto del Banco Mundial) se omiten con `skiprows=4` mediante pandas
> antes de crear el DataFrame de Spark.

---

## 4. ⚠️ Nota Crítica: `precio_red_estimado_usd` es una Imputación Sintética

> **DECLARACIÓN OBLIGATORIA:**
> La columna `precio_red_estimado_usd` presente en el dataset Silver
> `global_petrol_prices` **es una imputación sintética basada en reglas de
> negocio** ante la **ausencia de series de tiempo de precios reales** en la
> capa Bronze.

### Heurística aplicada

El precio base es el campo `residential_usd_per_kwh` (promedio estático
2023–2026). Se le aplica un factor de estacionalidad dependiendo del mes
en que se ejecuta el job de ingesta:

| Meses           | Hemisferio Norte | Factor   | Justificación                          |
|-----------------|------------------|----------|----------------------------------------|
| Junio, Julio, Agosto | Verano      | `+15%`   | Pico de demanda por aire acondicionado |
| Diciembre, Enero, Febrero | Invierno | `+12%`  | Calefacción eléctrica y días cortos    |
| Resto del año   | Temporada media  | `+5%`    | Fluctuación de base                    |

### Limitaciones conocidas

1. **No distingue hemisferio Sur**: un mismo factor de verano norte se aplica
   a Argentina o Chile en junio, cuando esos países están en invierno.
2. **Es un proxy grueso**: no refleja precios de contratos industriales,
   variaciones intra-día ni respuesta de la demanda.
3. **No sustituye datos reales**: si en el futuro se integran series de precios
   con granularidad temporal (ej. ESIOS en España, CAISO en California), esta
   columna debe **recalcularse o marcarse como obsoleta**.

### Uso recomendado en Gold

Utilizar `precio_red_estimado_usd` únicamente para análisis comparativos de
**orden de magnitud** entre países. No debe usarse para proyecciones financieras
precisas ni para reportes regulatorios.

---

## 5. Criterios de Calidad (CA Jira)

Implementados en `feature/quality-silver-validation/validations.py`:

| Check                          | Dataset               | Umbral                |
|--------------------------------|-----------------------|-----------------------|
| `energy_consumed_kwh > 0`      | `usage_logs`          | 0 registros fallidos  |
| `session_id` único             | `usage_logs`          | 0 duplicados          |
| `gpu_utilization` en [0,1]     | `usage_logs`          | 0 registros fuera     |
| `job_type` en dominio válido   | `usage_logs`          | 0 desconocidos        |
| `execution_status` en dominio  | `usage_logs`          | 0 desconocidos        |
| `timestamp` no nulo            | `usage_logs`          | 0 nulos               |
| `carbon_intensity` en [0,1000] | `carbon_intensity/*`  | 0 registros fallidos  |
| `zone` y `datetime` no nulos   | `carbon_intensity/*`  | 0 nulos               |
| `residential_usd_per_kwh ≥ 0`  | `global_petrol_prices`| 0 negativos           |
| `precio_red` consistente       | `global_petrol_prices`| 0 nulos injustificados|
| `carbon_intensity_elec ≥ 0`    | `owid`                | 0 negativos           |
| `year` en [1900, 2030]         | `owid`                | 0 fuera de rango      |
| `country` no nulo              | `owid`                | 0 nulos               |
| `ict_exports_usd ≥ 0`          | `world_bank`          | 0 negativos           |
| `year` en [1960, 2025]         | `world_bank`          | 0 fuera de rango      |
| `country_code` no nulo         | `world_bank`          | 0 nulos               |
| `price_usd_per_hour > 0`       | `ec2_pricing`         | 0 registros fallidos  |
| `instance_type` no nulo        | `ec2_pricing`         | 0 nulos               |
| `cloud_region` no nulo         | `ec2_pricing`         | 0 nulos               |

---

## 6. Estrategia de Particionado Silver

Definida centralmente en `feature/storage-partitioning-silver/writer.py`:

| Dataset                                    | Columnas de Partición       |
|--------------------------------------------|-----------------------------|
| `electricity_maps/carbon_intensity/*`      | `zone`, `year`, `month`     |
| `electricity_maps/electricity_mix/latest`  | `zone`, `year`, `month`     |
| `electricity_maps/zones/catalog`           | `country_code`              |
| `usage_logs`                               | `year`, `month`, `region`   |
| `owid`                                     | `year`                      |
| `world_bank/ict_exports`                   | `year`                      |
| `mlco2/yearly_averages`                    | `year`                      |
| `reference/ec2_pricing`                    | `cloud_provider`            |
| `global_petrol_prices`                     | *(sin partición — 145 filas)*|

---

## 7. Auditoría Bronze vs Silver

El script `feature/audit-bronze-silver/audit.py` genera una tabla de auditoría
persistida en `s3://SILVER/audit/audit_log/` particionada por `run_date`.

**Umbrales de alerta:**
- Pérdida > 5% de registros entre Bronze y Silver → `WARN_HIGH_LOSS`.
- Silver tiene más filas que Bronze → `INFO_EXPANDED` (normal en datasets
  con arrays que se expanden mediante `explode()`).
- Bronze o Silver vacío → `WARN_EMPTY_BRONZE`.

---

## 8. Supuestos Transversales

- Todos los timestamps se almacenan en **UTC**.
- Los tipos de datos son explícitos en `schemas.py`; no se usa `inferSchema=true`
  en producción.
- Las variables de entorno se cargan desde `.env` en desarrollo local y desde
  AWS Secrets Manager / SSM Parameter Store en producción.
- El esquema del catálogo de zonas de Electricity Maps puede cambiar con el
  tiempo (API pública); revalidar periódicamente.
