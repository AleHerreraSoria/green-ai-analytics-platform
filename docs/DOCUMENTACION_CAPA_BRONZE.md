# Documentación técnica — capa Bronze (ingesta)

Guía operativa de Bronze: qué se genera, qué se carga a S3 y cómo está organizado el bucket. Incluye `usage_logs`, catálogos `reference/` y la ingesta JSON de Electricity Maps.

---

## 1. Rol de la capa Bronze

Bronze almacena datos crudos o mínimamente transformados para trazabilidad y reproceso. Silver limpia/normaliza y Gold sirve analítica de negocio.

| Aspecto | Criterio en este repo |
|--------|------------------------|
| Formato | CSV UTF-8 con cabecera y JSON (Electricity Maps). |
| Ubicación | Local: `bronze/<dataset>/`. S3: prefijos en `s3://green-ai-pf-bronze-a0e96d06/`. |
| Contrato de datos | Definido en `docs/DICCIONARIO_DE_DATOS.md` (§4 logs, §7 mapeo geo, §8 precios EC2). |

---

## 2. Dataset: logs sintéticos de uso de IA (`usage_logs`)

Objetivo: simular sesiones de cómputo con claves compatibles con MLCO2 (`gpus.csv`, `instances.csv`, `impact.csv`) para evitar joins vacíos en capas posteriores.

Artefacto:

| Elemento | Valor |
|----------|--------|
| Script | `scripts/generate_synthetic_usage_logs.py` |
| Salida por defecto | `bronze/usage_logs/usage_logs.csv` |
| Dependencia | `faker` |
| Semilla | `random` y `Faker` se inicializan con `--seed` para reproducibilidad |

Prerrequisitos (modo estricto):

| Archivo | Uso |
|---------|-----|
| `gpus.csv` | Catálogo de GPU y TDP. |
| `instances.csv` | Relación instancia AWS ↔ GPU. |
| `impact.csv` | Fuente de regiones AWS válidas. |

Ruta por defecto: `data/Code_Carbon/` (override: `--mlco2-dir`).  
Si falta un insumo, falla (salvo `--allow-fallback`, solo para pruebas).

Puntos de diseño:
- Regiones solo desde `impact.csv`.
- Validación de coherencia instancia/GPU al inicio.
- Movilidad regional por usuario con `--mobility-regions-per-user`.
- Filas sucias controladas con `--edge-case-rate` para pruebas de calidad en Silver.

Esquema principal del CSV:

| Columna | Tipo lógico | Notas |
|---------|-------------|--------|
| `session_id` | string (UUID) | Identificador único de sesión. |
| `user_id` | string | `USER_000` … `USER_499`. |
| `timestamp` | string | Inicio de tarea; formato `YYYY-MM-DD HH:MM:SS` (UTC en la generación). |
| `gpu_model` | string | Nombre canónico MLCO2 (`gpus.name`). |
| `region` | string | Código AWS presente en `impact.csv`. |
| `duration_hours` | float, vacío o negativo | Ver §2.6. |
| `instance_type` | string | `instances.id` (AWS). |
| `gpu_utilization` | float | Entre 0.1 y 1.0. |
| `job_type` | string | `Training`, `Inference`, `Fine-tuning`. |
| `energy_consumed_kwh` | float o vacío | `(duration_hours × TDP_W × gpu_utilization) / 1000` si la duración es válida. |
| `execution_status` | string | `Success` (~95 %) o `Failed` (~5 %). |

Definición de negocio y ejemplos: `docs/DICCIONARIO_DE_DATOS.md` (§4).

---

## 3. Línea de comandos

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
```

| Parámetro | Descripción |
|-----------|-------------|
| `--rows` | Número de filas (default 50000). |
| `--seed` | Semilla reproducible. |
| `--mlco2-dir` | Directorio con `gpus.csv`, `instances.csv`, `impact.csv`. |
| `--output` | Ruta del CSV de salida. |
| `--allow-fallback` | Permite catálogos mínimos si faltan CSV (solo pruebas). |
| `--edge-case-rate` | Fracción [0,1] de filas con duración/energía inválidas (0 = ninguna). |
| `--mobility-regions-per-user` | Tamaño del pool de regiones por usuario. |

---

## 4. Almacenamiento en Amazon S3 (Bronze)

Los datasets actuales y la ingesta de Electricity Maps ya están cargados en el bucket Bronze.

### 4.1 Scripts involucrados

| Script | Función |
|--------|---------|
| `scripts/generate_synthetic_usage_logs.py` | Genera `bronze/usage_logs/usage_logs.csv`. |
| `scripts/build_aws_ec2_pricing_reference.py` | Regenera `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv`. |
| `scripts/upload_bronze_to_s3.py` | Sube CSV al bucket Bronze (`AWS_S3_BUCKET`, `AWS_PROFILE`, etc.). |
| `scripts/ingest_electricity_maps.py` | Consume API y sube JSON al prefijo `electricity_maps/`. |

Comandos típicos:

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
python scripts/upload_bronze_to_s3.py
```

### 4.2 Organización del bucket

**Nombre del bucket:** `green-ai-pf-bronze-a0e96d06`  
**URI base:** `s3://green-ai-pf-bronze-a0e96d06/`

En S3, las carpetas son prefijos:

```
s3://green-ai-pf-bronze-a0e96d06/
├── electricity_maps/          # JSON API Electricity Maps (`scripts/ingest_electricity_maps.py`; §6)
├── global_petrol_prices/
├── mlco2/
├── owid/
├── reference/                 # geo_cloud_to_country_and_zones.csv + aws_ec2_on_demand_usd_per_hour.csv
├── usage_logs/
└── world_bank/
```

Documentos por prefijo:

| Carpeta (prefijo) | Documento(s) en el bucket | Origen en el repositorio |
|-------------------|---------------------------|---------------------------|
| `electricity_maps/` | `electricity_mix/latest`, `carbon_intensity/{latest,past,history,past-range}`. | `scripts/ingest_electricity_maps.py` (carga ya operativa en este bucket). |
| `reference/` | `geo_cloud_to_country_and_zones.csv` | `bronze/reference/geo_cloud_to_country_and_zones.csv` (diccionario §7). |
| `reference/` | `aws_ec2_on_demand_usd_per_hour.csv` | `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` (diccionario §8). |
| `global_petrol_prices/` | `electricity_prices_by_country_2023_2026_avg.csv` | `data/Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` |
| `mlco2/` | `gpus.csv` | `data/Code_Carbon/gpus.csv` |
| `mlco2/` | `instances.csv` | `data/Code_Carbon/instances.csv` |
| `mlco2/` | `impact.csv` | `data/Code_Carbon/impact.csv` |
| `mlco2/` | `2021-10-27yearly_averages.csv` | `data/Code_Carbon/2021-10-27yearly_averages.csv` |
| `owid/` | `owid-energy-data.csv` | `data/Our_World_In_Data/owid-energy-data.csv` |
| `usage_logs/` | `usage_logs.csv` | `bronze/usage_logs/usage_logs.csv` |
| `world_bank/` | `API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` | `data/World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` |
| `world_bank/` | `Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` | `data/World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` |

Ejemplos de clave S3 completa:

- `s3://green-ai-pf-bronze-a0e96d06/usage_logs/usage_logs.csv`
- `s3://green-ai-pf-bronze-a0e96d06/reference/geo_cloud_to_country_and_zones.csv`
- `s3://green-ai-pf-bronze-a0e96d06/reference/aws_ec2_on_demand_usd_per_hour.csv`

Estructura de carpetas (prefijos) relevante dentro de `electricity_maps/`:

```
s3://green-ai-pf-bronze-a0e96d06/electricity_maps/
├── electricity_mix/
│   └── latest/zone=<ZONE>/<run_slug>.json
└── carbon_intensity/
    ├── latest/zone=<ZONE>/<run_slug>.json
    ├── past/zone=<ZONE>/datetime=<ISO>/<run_slug>.json
    ├── history/zone=<ZONE>/<run_slug>.json
    └── past-range/zone=<ZONE>/start=<ISO>/end=<ISO>/<run_slug>.json
```

Si se agrega un nuevo dataset, actualizar `scripts/upload_bronze_to_s3.py` y esta tabla.

---

## 5. Versionado y almacenamiento

- Los CSV grandes suelen excluirse del versionado; el contrato vive en el diccionario y esta documentación.
- S3 es el almacén operativo para consumo en Athena/Spark.

---

## 6. Próximas extensiones

- **Electricity Maps API:** mantener corrida programada de `scripts/ingest_electricity_maps.py` (token en `.env`).
- **Particionado** por fecha en `usage_logs/` (`year=/month=/day=`) si el volumen o el motor lo requieren.
- **Metadatos** de carga (run id, checksum, fecha de ingesta).

---

## 7. Referencias cruzadas

- `docs/DICCIONARIO_DE_DATOS.md` — §4 Generador de logs (sintético), §4.0 principios; **§7** `geo_cloud_to_country_and_zones.csv`; **§8** `aws_ec2_on_demand_usd_per_hour.csv`.
- `docs/PREGUNTAS_DE_NEGOCIO.md` — uso de logs, mapeo geográfico y precios EC2 en preguntas de coste / huella.
- `scripts/generate_synthetic_usage_logs.py` — implementación fuente de verdad del comportamiento descrito.
- `scripts/build_aws_ec2_pricing_reference.py` — generación del catálogo de precios EC2 en `bronze/reference/`.
- `scripts/ingest_electricity_maps.py` — ingesta Bronze desde la API Electricity Maps hacia `electricity_maps/` en S3.
- `scripts/upload_bronze_to_s3.py` — carga de CSV al bucket Bronze en S3 (incluye ambos archivos bajo `reference/`).
