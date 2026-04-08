# Documentación técnica — capa Bronze (ingesta)

Este documento centraliza la **documentación técnica de la ingesta y materialización en Bronze** del proyecto. Se irá ampliando conforme se añadan fuentes (S3, APIs, particionado Hive, etc.). La primera entrega cubre los **logs sintéticos de uso de IA** generados con el script basado en Faker.

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

## 4. Versionado y almacenamiento

- Los CSV grandes de Bronze suelen **excluirse del control de versiones** (política del equipo / `.gitignore`). El **contrato** queda en el diccionario de datos y en este documento; la **reproducción** del dataset es ejecutando el script con la misma semilla y los mismos CSV MLCO2.
- Si en el futuro se adopta **particionado** por fecha (`year=/month=/day=`) o carga a **S3**, documentar aquí la convención de rutas, el formato de partición y el job que escribe Bronze.

---

## 5. Próximas extensiones (plantilla)

Las siguientes secciones se pueden añadir cuando exista implementación:

- Ingesta desde **Amazon S3** (bucket, prefijo, credenciales, formato).
- **Particionado** y esquema de directorios bajo `bronze/usage_logs/`.
- **Metadatos** (Airflow DAG id, run id, checksum).
- Otros datasets Bronze (p. ej. respuestas crudas de API Electricity Maps).

---

## 6. Referencias cruzadas

- `docs/DICCIONARIO_DE_DATOS.md` — §4 Generador de logs (sintético), §4.0 principios.
- `docs/PREGUNTAS_DE_NEGOCIO.md` — uso de logs en preguntas de movilidad y huella.
- `scripts/generate_synthetic_usage_logs.py` — implementación fuente de verdad del comportamiento descrito.
