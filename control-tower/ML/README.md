# ML — Descripción

Esta carpeta es el “cerebro estadístico” que mira el historial de tu pipeline en Airflow y aprende patrones para **anticipar qué tareas tienen más riesgo de fallar** en la próxima ejecución. No magia: es como tener un asistente que leyó muchas ejecuciones pasadas (qué tarea, a qué hora empezó, si fue reintento, cuánto suele durar cada cosa…) y te dice, para el run más reciente, **dónde conviene poner más atención** porque la probabilidad de fallo sube.

Además guarda números de referencia de duración por tarea (mediana y percentil 95 del entrenamiento) para marcar si una tarda **inusualmente más de lo normal**, no solo si falló.

En resumen: **descarga datos del orquestador, entrena un modelo sencillo y lo usa en el Control Tower** para mostrar riesgo y anomalías de duración en el último `dag run`.

---

## Vista técnica

### Flujo de datos

1. **`export_airflow_metadata.py`** llama a la API REST de Airflow y escribe un histórico de `dag runs` y `task instances` en Parquet (por defecto `ML/data/airflow_task_instances.parquet`). Requiere las mismas variables de entorno que el resto del Control Tower (`AIRFLOW_API_BASE_URL`, `PIPELINE_DAG_ID`, credenciales).
2. **`train_pipeline_ml.py`** lee ese Parquet, construye la tabla etiquetada y entrena el clasificador; guarda el artefacto en `ML/artifacts/pipeline_failure_classifier.joblib` y métricas en `training_metrics.json`.
3. **`scoring.py`** (y opcionalmente **`score_latest_run.py`**) cargan el histórico local, combinan con el **último run** obtenido en vivo desde Airflow, enriquecen features como en entrenamiento y devuelven por tarea una probabilidad de fallo `p_fail` y la bandera `duration_anomaly` (duración actual vs p95 del train por `task_id`).
4. **`refresh_training.py`** encadena export + entrenamiento (útil desde la UI: “actualizar y reentrenar”).

### Qué predice el modelo

- **Objetivo binario:** `y_fail = 1` si la tarea terminó en estado `failed` (solo filas con estado terminal y duración disponible).
- **Modelo:** `RandomForestClassifier` dentro de un `Pipeline` de scikit-learn: preprocesado con **One-Hot** sobre `task_id` y passthrough numérico sobre el resto de columnas.
- **Features principales** (`FEATURE_COLUMNS` en `features.py`):
  - `task_id` (categoría),
  - `start_hour`, `start_dow` (calendario del inicio),
  - `try_number`,
  - `task_order_idx` (posición fija en el DAG definida en `task_order.py`),
  - `median_duration_same_task_prior` (mediana histórica **expanding** de duraciones de la misma tarea, sin mirar el evento actual; captura “cuánto suele tardar esto” antes de esta instancia).

### Validación y casos límite

- El split train/test es **por `dag_run_id` en el tiempo** (los runs más recientes van a test), para no filtrar información del futuro dentro del mismo run.
- Si hay **pocos runs** o las clases están desbalanceadas en el tramo de train, el código **ajusta** qué runs van a train vs test para intentar tener éxitos y fallos en entrenamiento y un test no vacío (queda documentado en `split_notes` dentro de `training_metrics.json`).
- Si en el train **solo hay una clase**, no se puede entrenar un bosque útil: se usa **`DegenerateFailureClassifier`**, que devuelve una probabilidad de fallo **constante** (frecuencia de fallos observada) para no romper el flujo; conviene **reentrenar** cuando el histórico tenga ambas clases.

### Artefactos y configuración

- Modelo: `artifacts/pipeline_failure_classifier.joblib`
- Métricas e información auxiliar (ROC-AUC cuando aplica, informe de clasificación, medianas/p95 de duración por tarea, notas del split): `artifacts/training_metrics.json`
- Rutas alternativas vía `.env`: `ML_HISTORY_PARQUET`, `ML_MODEL_PATH`, `ML_METRICS_PATH` (ver `ml_package.py`).

### Dependencias

Listadas en `requirements.txt` de esta carpeta; el entrenamiento usa **pandas**, **scikit-learn**, **joblib** y lectura **Parquet**.
