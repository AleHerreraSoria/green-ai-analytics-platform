"""
Observabilidad ML: riesgo de fallo por tarea y anomalía de duración (último run Airflow).
Usa el mismo `.env` que Pipeline Live y rutas bajo `control-tower/ML/`.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from utils.project_env import ensure_dotenv_loaded

ensure_dotenv_loaded()

from ML.ml_package import ml_package_dir, ml_paths_summary
from ML.scoring import score_latest_run_dataframe


st.set_page_config(page_title="Pipeline ML — Observabilidad", layout="wide")
st.title("Observabilidad ML del pipeline")
st.caption(
    "Modelo entrenado con metadatos históricos de Airflow; requiere export + train en el servidor."
)

base = ml_package_dir()
default_hist = base / "data" / "airflow_task_instances.parquet"
default_model = base / "artifacts" / "pipeline_failure_classifier.joblib"

with st.expander("Rutas y variables", expanded=False):
    st.markdown(ml_paths_summary())
    st.code(
        "python ML/export_airflow_metadata.py\n"
        "python ML/train_pipeline_ml.py\n"
        "python ML/score_latest_run.py",
        language="text",
    )

col_a, col_b = st.columns(2)
with col_a:
    st.metric("Histórico Parquet", "OK" if default_hist.is_file() else "Falta export")
with col_b:
    st.metric("Modelo joblib", "OK" if default_model.is_file() else "Falta train")

if st.button("Calcular score del último dag run", type="primary"):
    with st.spinner("Consultando Airflow y aplicando el modelo…"):
        df, err = score_latest_run_dataframe()
    if err:
        st.error(err)
    elif df is not None:
        rid = str(df.iloc[0]["dag_run_id"])
        st.success(f"Run analizado: `{rid}`")
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "p_fail": st.column_config.NumberColumn(
                    "P(fallo)",
                    format="%.3f",
                    help="Probabilidad estimada de clase fallo (modelo batch).",
                ),
                "duration_sec": st.column_config.NumberColumn(
                    "Duración (s)",
                    format="%.1f",
                ),
                "duration_anomaly": st.column_config.CheckboxColumn(
                    "Duración > p95 (train)",
                    help="Duración mayor que el percentil 95 histórico del train por tarea.",
                ),
            },
        )
else:
    st.info(
        "Pulsá el botón para cargar el último estado desde la API de Airflow "
        "y evaluar tareas con modelo + histórico local."
    )
