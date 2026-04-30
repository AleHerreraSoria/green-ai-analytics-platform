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
from ML.refresh_training import refresh_ml_artifacts
from ML.scoring import score_latest_run_dataframe

st.set_page_config(page_title="Pipeline ML — Observabilidad", layout="wide")

# Botón principal: mismo esquema de color que Pipeline Live (sin agrandar tipografía ni padding).
st.markdown(
    """
    <style>
    section[data-testid="stMain"] button[kind="tertiary"] {
        background: #0a0a0a !important;
        background-color: #0a0a0a !important;
        border: 1px solid rgba(222, 255, 154, 0.32) !important;
        box-shadow: none !important;
        color: #deff9a !important;
    }
    section[data-testid="stMain"] button[kind="tertiary"] p {
        color: inherit !important;
    }
    section[data-testid="stMain"] button[kind="tertiary"]:hover,
    section[data-testid="stMain"] button[kind="tertiary"]:focus-visible {
        background: #0a0a0a !important;
        border-color: rgba(158, 220, 104, 0.55) !important;
        color: #9edc68 !important;
    }
    section[data-testid="stMain"] button[kind="tertiary"]:hover p,
    section[data-testid="stMain"] button[kind="tertiary"]:focus-visible p {
        color: #9edc68 !important;
    }
    section[data-testid="stMain"] button[kind="tertiary"]:active {
        background-color: #0a0a0a !important;
        color: #b8f070 !important;
    }
    section[data-testid="stMain"] button[kind="tertiary"]:active p {
        color: #b8f070 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "ml_refresh_notice" not in st.session_state:
    st.session_state["ml_refresh_notice"] = None

st.title("Observabilidad ML del pipeline")
st.caption(
    "Modelo entrenado con metadatos históricos de Airflow; requiere export + train en el servidor."
)

_notice = st.session_state.pop("ml_refresh_notice", None)
if _notice == "ok":
    st.success("Histórico Parquet y modelo actualizados correctamente.")

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

if st.button(
    "Actualizar histórico y reentrenar modelo",
    help="Ejecuta export desde la API de Airflow y luego el entrenamiento. "
    "Puede tardar varios minutos según el historial.",
):
    with st.spinner("Exportando metadatos y entrenando modelo… (puede tardar)"):
        ok, log_text = refresh_ml_artifacts()
    if ok:
        st.session_state["ml_refresh_notice"] = "ok"
    else:
        st.error("La actualización falló. Revisá la salida de los comandos.")
    with st.expander("Salida de export y train", expanded=True):
        st.code(log_text, language="text")
    if ok:
        st.rerun()

col_a, col_b = st.columns(2)
with col_a:
    st.metric("Histórico Parquet", "OK" if default_hist.is_file() else "Falta export")
with col_b:
    st.metric("Modelo joblib", "OK" if default_model.is_file() else "Falta train")

if st.button(
    "Calcular score del último dag run",
    type="tertiary",
    key="ml_score_last_run",
):
    with st.spinner("Consultando Airflow y aplicando el modelo…"):
        df, err = score_latest_run_dataframe()
    if err:
        st.error(err)
    elif df is not None:
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "p_fail": st.column_config.NumberColumn(
                    "P(fallo)",
                    format="%.3f",
                    help="Probabilidad estimada de fallo (0–1) con las features actuales e histórico.",
                ),
                "duration_sec": st.column_config.NumberColumn(
                    "Duración (s)",
                    format="%.1f",
                    help="Segundos de ejecución; −1 = aún sin duración (p. ej. tarea en curso o sin cierre en la API).",
                ),
                "duration_anomaly": st.column_config.CheckboxColumn(
                    "Duración > p95 (train)",
                    help="Duración mayor que el percentil 95 histórico del train por tarea.",
                ),
            },
        )
