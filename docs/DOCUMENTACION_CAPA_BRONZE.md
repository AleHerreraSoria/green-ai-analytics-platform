# Documentación técnica — capa Bronze (ingesta)

Este documento centraliza la **documentación técnica de la ingesta y materialización en Bronze** del proyecto. Cubre la generación de **logs sintéticos** (Faker), la **carga de CSV al bucket S3** y el inventario por prefijo; se ampliará con la API Electricity Maps y otros pipelines.

---

## 1. Rol de la capa Bronze

En el diseño del lakehouse del repositorio, **Bronze** almacena datos **crudos o mínimamente transformados**, tal como se generan o reciben, para trazabilidad y reprocesamiento. La capa **Silver** aplica limpieza, validación y normalización; **Gold** responde preguntas de negocio.

| Aspecto | Criterio en este repo |
|--------|------------------------|
| Formato | CSV UTF-8 con cabecera (implementación actual de usage logs). |
| Ubicación | `bronze/<dataset>/` bajo la raíz del repositorio (o equivalente en bucket S3 en despliegue). |
| Contrato de datos | Definido en `docs/DICCIONARIO_DE_DATOS.md` (sección 4 y §4.0). |

---

## 2. Dataset: logs sintéticos de uso de IA (`usage_logs`)

### 2.1 Objetivo

Simular **sesiones de cómputo** (entrenamiento / inferencia / fine-tuning) con campos alineados a catálogos **MLCO2** (`gpus.csv`, `instances.csv`, `impact.csv`) para que los joins posteriores (p. ej. en Spark) con factores de emisión y hardware **no produzcan resultados vacíos por claves inventadas**.

### 2.2 Artefacto generado

| Elemento | Valor |
|----------|--------|
| Script | `scripts/generate_synthetic_usage_logs.py` |
| Salida por defecto | `bronze/usage_logs/usage_logs.csv` |
| Dependencia Python | `faker` (ver `requirements.txt`) |
| Semilla | `random` y `Faker` se inicializan con `--seed` para reproducibilidad |

### 2.3 Prerrequisitos (modo estricto, por defecto)

El script espera una carpeta MLCO2 con al menos:

| Archivo | Uso |
|---------|-----|
| `gpus.csv` | Filas `type=gpu` con `tdp_watts` numérico; resolución del modelo de GPU y TDP para energía. |
| `instances.csv` | Filas `provider=aws`; pares `(id, gpu)` para `instance_type` y acoplamiento con GPU. |
| `impact.csv` | Filas `provider=aws`; columna `region` como **única fuente** de códigos de región. |

Ruta por defecto: `<raíz-repo>/data/Code_Carbon/`. Se puede sobrescribir con `--mlco2-dir`.

Si falta alguno de estos insumos, el script **termina con error** salvo que se use `--allow-fallback` (solo recomendado para pruebas sin CSV; los valores incrustados **no** garantizan consistencia con tu `impact.csv` real).

### 2.4 Validación de integridad en arranque

- **Instancias vs GPUs:** cada `gpu` declarado en `instances.csv` (AWS) debe poder mapearse a una entrada de `gpus.csv` (coincidencia exacta de `name` o prefijo, según la lógica del script). Si no, el proceso falla con mensaje explícito.
- **Regiones:** nunca se generan regiones fuera del conjunto AWS extraído de `impact.csv` (evita el antipatrón “región inventada” que rompe el join con factores de carbono).

### 2.5 Movilidad regional

Cada uno de los **500** `user_id` (`USER_000` … `USER_499`) recibe un **pool** de varias regiones (por defecto **4**), todas elegidas **solo** del catálogo `impact.csv`. En cada sesión, la región se elige **dentro del pool de ese usuario**. Así, un mismo usuario puede tener historial en regiones distintas (p. ej. zonas con distinta intensidad de carbono), lo que habilita análisis de “desplazamiento” de cómputo en capas posteriores o en dashboards.

Parámetro: `--mobility-regions-per-user` (mínimo efectivo 2 si el catálogo tiene al menos 2 regiones).

### 2.6 Datos deliberadamente sucios (Bronze)

Aproximadamente **`--edge-case-rate`** de las filas (por defecto **1 %**) llevan:

- `duration_hours` **vacío** o **negativo**, y  
- `energy_consumed_kwh` **vacío** (no se calcula energía a partir de duración inválida).

Sirve para demostrar reglas de calidad y limpieza en **Silver** (Spark, Great Expectations, etc.). Con `--edge-case-rate 0` se desactiva.

### 2.7 Esquema del CSV (orden de columnas)

Orden fijo de cabecera (alineado al diccionario de datos §4.1 y §4.2):

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

La definición de negocio y ejemplos adicionales están en `docs/DICCIONARIO_DE_DATOS.md` (§4 y §4.0).

---

## 3. Línea de comandos

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
```

| Parámetro | Descripción |
|-----------|-------------|
| `--rows` | Número de filas (por defecto 50000; típico 10k–100k). |
| `--seed` | Semilla reproducible. |
| `--mlco2-dir` | Directorio con `gpus.csv`, `instances.csv`, `impact.csv`. |
| `--output` | Ruta del CSV de salida. |
| `--allow-fallback` | Usa catálogos mínimos si faltan CSV (no usar en pipelines con MLCO2 real). |
| `--edge-case-rate` | Fracción [0,1] de filas con duración/energía inválidas (0 = ninguna). |
| `--mobility-regions-per-user` | Tamaño del pool de regiones por usuario. |

---

## 4. Almacenamiento en Amazon S3 (Bronze)

Los datasets actuales del repositorio (más los logs sintéticos) se **subieron al bucket** usando el script de carga. La generación local de los logs y la subida son **pasos distintos** (dos scripts).

### 4.1 Scripts involucrados

| Script | Función |
|--------|---------|
| `scripts/generate_synthetic_usage_logs.py` | Genera el CSV de sesiones en `bronze/usage_logs/usage_logs.csv` (Faker + catálogos MLCO2 locales). |
| `scripts/upload_bronze_to_s3.py` | Sube al bucket los archivos listados abajo; lee variables desde `.env` en la raíz del repo (`python-dotenv`): `AWS_S3_BUCKET`, `AWS_DEFAULT_REGION`, credenciales o `AWS_PROFILE`. |

Comandos típicos:

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_usage_logs.py --rows 50000 --seed 42
python scripts/upload_bronze_to_s3.py
```

### 4.2 Organización del bucket

**Nombre del bucket:** `green-ai-pf-bronze-a0e96d06`  
**URI base:** `s3://green-ai-pf-bronze-a0e96d06/`

En S3, las “carpetas” son **prefijos**. La primera jerarquía del bucket (sin carpeta intermedia `bronze/`) queda organizada así:

```
s3://green-ai-pf-bronze-a0e96d06/
├── electricity_maps/          # reservado: ingesta API Electricity Maps (pendiente; §6)
├── global_petrol_prices/
├── mlco2/
├── owid/
├── reference/                 # catálogos de referencia (mapeo geográfico, etc.)
├── usage_logs/
└── world_bank/
```

**Documento guardado por carpeta (prefijo)** — mismo contenido que sube `scripts/upload_bronze_to_s3.py`:

| Carpeta (prefijo) | Documento(s) en el bucket | Origen en el repositorio |
|-------------------|---------------------------|---------------------------|
| `electricity_maps/` | *Ninguno aún* (reservado para JSON/CSV de la API Electricity Maps). | — |
| `reference/` | `geo_cloud_to_country_and_zones.csv` | `bronze/reference/geo_cloud_to_country_and_zones.csv` |
| `reference/` | `aws_ec2_on_demand_usd_per_hour.csv` | `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` (regenerar: `python scripts/build_aws_ec2_pricing_reference.py`) |
| `global_petrol_prices/` | `electricity_prices_by_country_2023_2026_avg.csv` | `data/Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` |
| `mlco2/` | `gpus.csv` | `data/Code_Carbon/gpus.csv` |
| `mlco2/` | `instances.csv` | `data/Code_Carbon/instances.csv` |
| `mlco2/` | `impact.csv` | `data/Code_Carbon/impact.csv` |
| `mlco2/` | `2021-10-27yearly_averages.csv` | `data/Code_Carbon/2021-10-27yearly_averages.csv` |
| `owid/` | `owid-energy-data.csv` | `data/Our_World_In_Data/owid-energy-data.csv` |
| `usage_logs/` | `usage_logs.csv` | `bronze/usage_logs/usage_logs.csv` (generado con `scripts/generate_synthetic_usage_logs.py`) |
| `world_bank/` | `API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` | `data/World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` |
| `world_bank/` | `Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` | `data/World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` |

Ejemplo de clave S3 completa: `s3://green-ai-pf-bronze-a0e96d06/usage_logs/usage_logs.csv`.

*Nota:* si añades más CSV al repo (p. ej. otro extracto del Banco Mundial), incorpóralos en `upload_bronze_to_s3.py` y actualiza esta tabla.

---

## 5. Versionado y almacenamiento

- Los CSV grandes de Bronze suelen **excluirse del control de versiones** (política del equipo / `.gitignore`). El **contrato** queda en el diccionario de datos y en este documento; la **reproducción** del dataset de logs es ejecutando `generate_synthetic_usage_logs.py` con la misma semilla y los mismos CSV MLCO2.
- La **copia en S3** actúa como almacén operativo para ingesta en motores (Athena, Spark, etc.); los objetos por prefijo están en §4.2.

---

## 6. Próximas extensiones

- **Electricity Maps API:** ingesta de respuestas JSON (o CSV derivado) bajo el prefijo `electricity_maps/`; script o DAG dedicado (pendiente).
- **Particionado** por fecha en `usage_logs/` (`year=/month=/day=`) si el volumen o el motor lo requieren.
- **Metadatos** de carga (run id, checksum, fecha de ingesta).

---

## 7. Referencias cruzadas

- `docs/DICCIONARIO_DE_DATOS.md` — §4 Generador de logs (sintético), §4.0 principios.
- `docs/PREGUNTAS_DE_NEGOCIO.md` — uso de logs en preguntas de movilidad y huella.
- `scripts/generate_synthetic_usage_logs.py` — implementación fuente de verdad del comportamiento descrito.
- `scripts/upload_bronze_to_s3.py` — carga de CSV al bucket Bronze en S3.
