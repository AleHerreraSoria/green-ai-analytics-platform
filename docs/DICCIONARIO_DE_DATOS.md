# Diccionario de datos del repositorio

Este documento describe el **stack de información** del proyecto: contexto histórico y económico (Our World in Data), intensidad de carbono en tiempo real (Electricity Maps), tablas maestras de hardware e impacto regional (MLCO2), **precios medios de electricidad por país**, **exportaciones de servicios TIC** (Banco Mundial / WDI), y sesiones de uso de IA generadas de forma sintética. Las rutas de archivo indican copias locales en el repositorio cuando aplica.

## Fuentes de datos (el stack de información)

| # | Fuente | Descripción | Acceso | Justificación de elección |
|---|--------|-------------|--------|---------------------------|
| 1 | **Our World in Data (Energy)** | Datos históricos por país: PIB, población y mezcla energética (renovables vs. fósiles). | [Dataset en GitHub (OWID)](https://github.com/owid/energy-data) | Contexto económico y base comparativa histórica para países de América. |
| 2 | **Electricity Maps API** | Intensidad de carbono (gCO₂eq/kWh) en tiempo real por zona geográfica; mix y catálogo de zonas. | [Electricity Maps](https://www.electricitymaps.com/) | Componente en tiempo real que justifica ingesta automatizada (p. ej. con Airflow). |
| 3 | **MLCO2 — hardware e impacto** | Especificaciones TDP de GPUs/CPUs y factores de emisión por región de cómputo (proyecto MLCO2 / *impact*). | [MLCO2 / impact (GitHub)](https://github.com/mlco2/impact) | Tabla maestra para pasar de uso de hardware a consumo energético y emisiones. |
| 4 | **Generador de logs (sintético)** | Registros de sesiones de uso de IA (usuario, GPU, región, horas, etc.). | Desarrollo propio | Control de volumen (p. ej. 100k+ filas) y simulación de movilidad entre regiones de cómputo. |
| 5 | **Precios de electricidad por país** | Tarifas medias residenciales y de negocio en USD/kWh (promedio 2023-2026) por país. | [GlobalPetrolPrices.com — Electricity prices](https://www.globalpetrolprices.com/electricity_prices/); copia local: `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` | Complementa el contexto energético con el **costo** de la electricidad a nivel país (distinto de intensidad de carbono o mix). |
| 6 | **Banco Mundial — WDI** | Exportaciones de servicios de tecnologías de la información y comunicación (TIC), balanza de pagos, US$ corrientes. | [Indicador BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD); archivos en `World_Bank_Group/` | Contexto macroeconómico del sector digital/TIC por economía y en el tiempo (series anuales). |

---
## 1. Our World in Data (Energy)

Datos históricos por país o región agregada. En el repositorio: `OWID/owid-energy-data.csv` y el codebook asociado.

### 1.1 Dataset principal (`OWID/owid-energy-data.csv`)

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 23232 |
| Columnas | 130 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Serie temporal de energía por país o región agregada (Our World in Data). Cada fila es un par (entidad geográfica, año). Los valores numéricos pueden estar vacíos donde no hay dato.

La definición de cada variable del dataset principal (título, descripción, unidad y fuente) se documenta en el archivo auxiliar `OWID/owid-energy-codebook.csv`, descrito en la sección 1.2.

### 1.2 Codebook de variables (`OWID/owid-energy-codebook.csv`)

| Metadato | Valor |
|----------|-------|
| Registros (una fila por columna del dataset principal) | 130 |
| Columnas | 5 (`column`, `title`, `description`, `unit`, `source`) |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |
| Relación | Una fila por variable de `owid-energy-data.csv`; no contiene series temporales, solo metadatos. |

### Descripción

Dataset auxiliar publicado junto al CSV principal por Our World in Data: catálogo de las 130 variables del archivo de energía. Cada fila enlaza el **nombre de columna** en el CSV con su **título**, **definición**, **unidad** (si aplica) y **fuente** de la serie. Sirve como diccionario de referencia al diseñar esquemas Silver/Gold o al interpretar columnas en análisis. El detalle literal de cada variable permanece en `OWID/owid-energy-codebook.csv` del repositorio (y en el dataset OWID en GitHub).

### Estructura del archivo (columnas del codebook)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `column` | texto | Nombre de la columna tal como aparece en `owid-energy-data.csv`. |
| `title` | texto | Etiqueta o título descriptivo de la variable. |
| `description` | texto | Definición y alcance de la medida. |
| `unit` | texto | Unidad de la magnitud (vacío si no aplica o es adimensional). |
| `source` | texto | Fuente o referencia de la serie (puede incluir enlaces). |

### Variables del dataset principal cubiertas por el codebook (resumen temático)

Las 130 entradas describen, entre otras, las siguientes familias de indicadores (los prefijos de nombre en el CSV siguen la misma lógica: combustible o tema + tipo de métrica):

| Ámbito | Contenido típico (ejemplos de prefijos / nombres) |
|--------|---------------------------------------------------|
| Identificación | `country`, `year`, `iso_code` |
| Economía y población | `population`, `gdp` |
| Energía agregada | `primary_energy_consumption`, `energy_per_capita`, `energy_per_gdp`, cambios anuales (`energy_cons_change_*`) |
| Electricidad (totales e importaciones) | `electricity_generation`, `electricity_demand`, `per_capita_electricity`, `net_elec_imports`, `carbon_intensity_elec`, `greenhouse_gas_emissions` |
| Por combustible / tecnología | Series por **carbón**, **petróleo**, **gas**, **nuclear**, **hidro**, **solar**, **eólica**, **biocombustibles**, **renovables**, **baja emisión** (`low_carbon_*`), **otras renovables**: consumo primario, generación eléctrica, shares, valores per cápita, producción (fósiles), y cambios interanuales (`*_cons_change_*`, `*_share_elec`, `*_share_energy`, etc.) |

Para el significado exacto, unidad y fuente de cada columna, consultar el CSV del codebook o unir `owid-energy-data.csv` con `owid-energy-codebook.csv` por `column`.

---

## 2. Electricity Maps API

Especificación de la **capa Silver** para respuestas JSON: intensidad de carbono (Latest / Past / History), mix eléctrico (Latest) y catálogo de zonas. Los endpoints de intensidad comparten la misma estructura base; en History se recibe una lista de objetos con ese esquema.

### 2.1 Intensidad de carbono (Latest / Past / History)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zone` | String | Código identificador de la zona eléctrica. | N/A | `"DE"` (Alemania) |
| `carbonIntensity` | Integer | Métrica clave: emisiones por unidad de energía. | gCO₂eq/kWh | `302` |
| `datetime` | Timestamp | Fecha y hora de la medición (ISO 8601). | UTC | `2026-04-06T21:00Z` |
| `emissionFactorType` | String | Metodología de cálculo (ciclo de vida o directo). | N/A | `"lifecycle"` |
| `isEstimated` | Boolean | Indica si el dato es real o una estimación. | N/A | `false` |
| `temporalGranularity` | String | Frecuencia del dato. | N/A | `"hourly"` |

Para **History**, en Spark conviene usar `explode()` sobre la lista de registros antes de materializar filas en almacenamiento.

### 2.2 Mix eléctrico (Latest)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zone` | String | Identificador de la zona geográfica. | N/A | `"FR"` (Francia) |
| `datetime` | Timestamp | Momento de la observación del mix. | UTC | `2026-03-18T12:00Z` |
| `nuclear` | Float | Energía generada por fuentes nucleares. | MW | `38906.35` |
| `solar` | Float | Energía generada por radiación solar. | MW | `13062.17` |
| `wind` | Float | Energía generada por fuerza eólica. | MW | `6866.17` |
| `coal` | Float | Energía generada por combustión de carbón. | MW | `0` |
| `gas` | Float | Energía generada por gas natural. | MW | `1486.62` |

### 2.3 Catálogo de zonas (helpers)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zoneKey` | String | Código único de la zona (ID para joins). | N/A | `"US-CAL-CISO"` |
| `zoneName` | String | Nombre del operador de red o región. | N/A | `"CAISO"` |
| `countryName` | String | País al que pertenece la zona. | N/A | `"USA"` |
| `countryCode` | String | Código de país estándar (2 letras). | N/A | `"US"` |

Nota: en la respuesta de **Zones**, la API puede devolver un objeto cuya llave es el nombre de la zona (p. ej. `"US-CAL-BANC": { ... }`). Conviene aplanar ese mapa a tabla con columna de zona explícita para cruces posteriores.

### 2.4 Rol en el lakehouse (resumen)

| Capa | Contenido |
| :--- | :--- |
| Bronze | JSON crudo tal cual llega de la API (anidado). |
| Silver | JSON aplanado según los diccionarios anteriores (p. ej. `mix.nuclear` → columna `nuclear`). |
| Gold | Cruce de intensidad de carbono con logs de uso de IA usando `zone` como clave. |

---

## 3. MLCO2 — tablas de hardware e impacto

Copias locales usadas como **tabla maestra**: instancias cloud, factores por región, especificaciones de chips y promedios anuales por zona. Proyecto MLCO2 (*impact*): ver enlace en la tabla de fuentes.

### 3.1 `MLCO2/instances.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 14 |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |

### Descripción

Relación de tipos de instancia de proveedores cloud (AWS) con modelo de GPU asociado y enlace a la fuente del fabricante o del proveedor.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `provider` | texto | Código corto del proveedor en minúsculas (p. ej. `aws`). |
| `id` | texto | Identificador del tipo de instancia (p. ej. `p3.2xlarge`). |
| `gpu` | texto | Modelo de GPU instalada en esa instancia. |
| `source` | texto (URL) | Enlace a la documentación del tipo de instancia. |

---


### 3.2 `MLCO2/impact.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 82 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Intensidad de carbono del suministro eléctrico por región de cómputo y proveedor (datos usados en la calculadora MLCO2). Fuentes: eGRID, Electricity Map, literatura, Carbon Footprint, etc. (véase `MLCO2/Readme.md`).

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `provider` | texto | Código corto en minúsculas del proveedor (obligatorio). |
| `providerName` | texto | Nombre completo del proveedor (obligatorio). |
| `offsetRatio` | entero | Porcentaje de emisiones de carbono compensadas por el proveedor en esa región (0–100) (obligatorio). |
| `region` | texto | Código corto en minúsculas de la región (obligatorio). |
| `regionName` | texto | Nombre legible de la región; por defecto igual a `name` si falta (obligatorio). |
| `country` | texto | País (puede estar vacío). |
| `state` | texto | Estado/provincia (puede estar vacío). |
| `city` | texto | Ciudad o área del datacenter (puede estar vacío). |
| `impact` | numérico | Gramos de CO₂ equivalente por kWh (gCO₂eq/kWh) (obligatorio). |
| `source` | texto | Referencia o URL de la fuente del factor de emisión. |
| `PUE` | numérico | Power Usage Effectiveness del datacenter cuando aplica (puede estar vacío). |
| `PUE source` | texto | Enlace o referencia para el valor PUE. |
| `comment` | texto | Notas adicionales (p. ej. datacenter específico, promedios por periodo). |

---


### 3.3 `MLCO2/gpus.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 48 |
| Formato | CSV, separador coma |
| Nota | La cabecera incluye un campo con espacio inicial: ` TFLOPS16` |

### Descripción

Especificaciones de rendimiento y consumo de GPUs y CPUs para estimación energética. Incluye modelos de consumo (TDP), TFLOPS, eficiencia, memoria y fuente.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `name` | texto | Nombre comercial del chip (GPU o CPU). |
| `type` | texto | Categoría: `gpu` o `cpu`. |
| `tdp_watts` | numérico | TDP o potencia de referencia en vatios. |
| `TFLOPS32` | numérico | Rendimiento en TFLOPS en precisión FP32 (puede ser NaN). |
| ` TFLOPS16` | numérico | Rendimiento en TFLOPS en FP16 (nombre de columna con espacio inicial; puede ser NaN). |
| `GFLOPS32/W` | numérico | Eficiencia GFLOPS/W en FP32. |
| `GFLOPS16/W` | numérico | Eficiencia GFLOPS/W en FP16. |
| `memory` | numérico | Memoria en GB cuando aplica (puede estar vacío). |
| `source` | texto (URL) | Enlace a ficha técnica o fuente de especificaciones. |

---


### 3.4 `MLCO2/2021-10-27yearly_averages.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 642 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Promedios anuales de intensidad de carbono por zona eléctrica, con cobertura horaria indicada.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `zone_key` | texto | Identificador de zona (p. ej. prefijo país-región como `AUS-NSW`). |
| `year` | entero | Año del promedio. |
| `Country` | texto | País. |
| `Zone Name` | texto | Nombre legible de la zona eléctrica o región. |
| `carbon_intensity_avg` | numérico | Intensidad media de carbono de la electricidad (valor numérico según metodología de la fuente). |
| `no_hours_with_data` | entero | Número de horas con datos disponibles usadas en el promedio del año. |

---

## 4. Generador de logs (sintético) — desarrollo propio

Tabla orientada a sesiones de cómputo; varios campos deben alinearse con las tablas MLCO2 de la sección 3 (GPUs, factores por región e instancias cloud).

### 4.0 Integridad referencial, movilidad y datos sucios (Bronze)

| Principio | Regla |
| :--- | :--- |
| **Match con MLCO2** | `region` se elige **solo** entre los códigos `region` de `impact.csv` con `provider = aws` (nunca valores inventados tipo `"Mexico-City"` si no existen en el catálogo, para que el join con factores de emisión no quede vacío). `gpu_model` debe resolverse a un `name` de `gpus.csv` coherente con el `gpu` de `instances.csv`; `instance_type` solo desde `instances.csv` (AWS). El script valida que cada instancia mapee a una GPU del catálogo. |
| **Movilidad regional** | Cada `user_id` tiene asignado un **subconjunto de varias regiones** muestreadas del mismo `impact.csv`. Las sesiones de ese usuario solo usan regiones de ese subconjunto, de modo que un mismo usuario aparece en **distintas regiones** a lo largo del tiempo (p. ej. `us-east-1` y `ca-central-1`) y se puede analizar desplazamiento de cómputo o ahorro de emisiones en Gold/dashboards. |
| **Casos borde (Bronze)** | Aprox. **1 %** de las filas llevan `duration_hours` **inválido**: celda vacía (NULL al ingerir) o **valor negativo**. En esas filas `energy_consumed_kwh` va **vacío** (no se calcula kWh con datos basura). La **Silver** debe filtrar o corregir estas filas (Spark, Great Expectations, etc.). |

### 4.1 Identificación, tiempo y recursos

| Columna | Tipo | Descripción | Lógica de generación | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `session_id` | String | ID único de la sesión de cómputo. | UUID o correlativo del script. | `a1b2-c3d4-e5f6` |
| `user_id` | String | Usuario o cliente. | Entre 500 usuarios ficticios (`USER_000`…`USER_499`); cada uno con varias regiones posibles (§4.0). | `USER_123` |
| `timestamp` | Timestamp | Inicio de la tarea de IA. | Fecha aleatoria entre 2024-01-01 y la fecha actual. | `2026-03-15 14:30:05` |
| `gpu_model` | String | Modelo de GPU de la sesión. | Nombre canónico del catálogo MLCO2 de GPUs (sección 3.3), alineado con la instancia elegida. | `Tesla V100-SXM2-16GB` |
| `region` | String | Región cloud del job. | Solo códigos presentes en factores MLCO2 AWS en `impact.csv` (sección 3.2), acotados al pool de movilidad del usuario. | `us-east-1` |
| `duration_hours` | Float o vacío | Horas con GPU activa. | En ~99 %: aleatorio (normal acotada) entre 0.1 y 24.0. En ~1 %: vacío o negativo (Bronze sucio; ver §4.0). | `2.5` |
| `instance_type` | String | Tipo de instancia AWS. | Debe coincidir con identificadores del catálogo MLCO2 de instancias (sección 3.1). | `p3.2xlarge` |

### 4.2 Utilización, tipo de job y energía

| Columna | Tipo | Lógica de generación | ¿Por qué es útil? |
| :--- | :--- | :--- | :--- |
| `gpu_utilization` | Float | Aleatorio entre 0.1 y 1.0 (10 % a 100 %). | Una GPU al 20 % no consume como al 100 %; ajusta la fórmula de emisiones. |
| `job_type` | String | `"Training"`, `"Inference"` o `"Fine-tuning"`. | Permite comparar impacto por tipo de carga (p. ej. entrenamiento vs inferencia). |
| `energy_consumed_kwh` | Float o vacío | Si `duration_hours` es válido: `(duration_hours × TDP × utilization) / 1000`. Si no, vacío. | Valor listo para Silver/Gold cuando la duración es confiable. |
| `execution_status` | String | `"Success"` (~95 %) o `"Failed"` (~5 %). | Mide energía asociada a ejecuciones fallidas. |

---

## 5. Precios de electricidad por país (`Global_Petrol_Prices`)

**Fuente original:** [GlobalPetrolPrices.com — Electricity prices around the world](https://www.globalpetrolprices.com/electricity_prices/). En esa página, la tabla *Compare Electricity Prices by Country* publica las medias de tarifas residenciales y de negocio en USD/kWh para el periodo **2023-2026** (promedios pensados para comparar países sin el ruido trimestral). Los precios por kWh incluyen partidas típicas de la factura (distribución, energía, cargos e impuestos según metodología del sitio).

El CSV del repositorio es una **copia local** con **una fila por país** y las mismas magnitudes. Útil para análisis económico del costo energético frente a consumo o emisiones.

### 5.1 `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv`

| Metadato | Valor |
|----------|-------|
| Fuente | [globalpetrolprices.com/electricity_prices/](https://www.globalpetrolprices.com/electricity_prices/) |
| Registros (filas de datos) | 145 |
| Columnas | 3 |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |
| Nota | En parte de los países la tarifa de negocio puede ir vacía en la fuente; en el archivo actual son 11 filas con el tercer campo vacío. |

### Descripción

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country` | texto | Nombre del país o territorio (orden del archivo no implica ranking; conviene ordenar en análisis por la métrica deseada). |
| `Residential electricity rate USD per kWh (2023-2026 average)` | numérico | Tarifa media **residencial** en USD por kWh, promediada sobre 2023-2026 según el criterio con el que se construyó el archivo. |
| `Business electricity rate USD per kWh (2023-2026 average)` | numérico | Tarifa media de **negocio** en USD por kWh, mismo periodo; puede estar vacío para algunos países. |

Los nombres de columna son largos; en pipelines (p. ej. Spark o pandas) suele renombrarse a algo como `residential_usd_per_kwh` y `business_usd_per_kwh` para facilitar consultas.

---

## 6. Banco Mundial — exportaciones de servicios TIC (`World_Bank_Group`)

**Fuente:** [World Development Indicators (WDI) — Indicator BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD). Los archivos CSV del repositorio reproducen la descarga estándar del API/dataset del Banco Mundial para ese indicador (nombres de archivo con sufijo `API_BX.GSR.CCIS.CD_...`).

**Indicador (código `BX.GSR.CCIS.CD`):** *ICT service exports (BoP, current US$)* — exportaciones de servicios de TIC según la balanza de pagos, en **dólares estadounidenses corrientes**. Incluye servicios de informática y comunicaciones (telecomunicaciones, postal/mensajería) y servicios de información (datos y transacciones vinculadas a noticias). **Organización de la fuente (metadatos):** Balance of Payments Statistics Yearbook y archivos de datos, Fondo Monetario Internacional (FMI).

### 6.1 Serie principal — `World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Fuente | [data.worldbank.org — BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD) |
| Registros (filas de datos) | 266 |
| Columnas | 71 |
| Formato | CSV; las primeras filas son metadatos del extracto (`Data Source`, `Last Updated Date`); la fila de cabecera de datos empieza en `Country Name`. |
| Codificación | UTF-8 |
| Periodo (columnas año) | 1960 a 2025 (una columna por año; celdas vacías = sin dato) |

#### Descripción de columnas

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country Name` | texto | Nombre de la economía (país o agregado regional, p. ej. *Arab World*, *Europe & Central Asia*). |
| `Country Code` | texto | Código de 3 letras (p. ej. `ARG`, `USA`) o código de grupo cuando aplica. |
| `Indicator Name` | texto | Nombre del indicador; en todos los registros: *ICT service exports (BoP, current US$)*. |
| `Indicator Code` | texto | Código único del indicador: `BX.GSR.CCIS.CD`. |
| `1960` … `2025` | numérico | Valor del indicador en USD corrientes para ese año; vacío si no existe observación. |

---

### 6.2 Metadatos del indicador — `World_Bank_Group/Metadata_Indicator_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Registros (definición del indicador) | 1 |
| Columnas | 4 |

| Campo | Descripción |
|-------|-------------|
| `INDICATOR_CODE` | `BX.GSR.CCIS.CD` |
| `INDICATOR_NAME` | Nombre largo del indicador (inglés). |
| `SOURCE_NOTE` | Definición ampliada: alcance de “servicios TIC” y moneda (USD corrientes). |
| `SOURCE_ORGANIZATION` | Entidad que provee la serie subyacente (p. ej. FMI / BoP). |

---

### 6.3 Metadatos por economía — `World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 265 |
| Columnas | 5 campos de datos (la cabecera exportada puede incluir BOM UTF-8 y una columna vacía por coma final) |

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country Code` | texto | Identificador de economía (alineable con `Country Code` del archivo principal). |
| `Region` | texto | Región del Banco Mundial (puede estar vacío en agregados). |
| `IncomeGroup` | texto | Clasificación por nivel de ingreso (puede estar vacío en agregados). |
| `SpecialNotes` | texto | Notas metodológicas, calendario fiscal, tipos de cambio, etc. (texto largo; puede ocupar varias líneas en el CSV). |
| `TableName` | texto | Nombre corto de la economía para tablas. |

Sirve para enriquecer la serie principal con **región**, **grupo de ingreso** y **notas** al hacer joins por `Country Code`.
