"""Rutas del paquete ML bajo control-tower (misma instancia que Streamlit)."""

from __future__ import annotations

import os
from pathlib import Path


def ml_package_dir() -> Path:
    return Path(__file__).resolve().parent


def ml_paths_summary() -> str:
    base = ml_package_dir()
    h = base / "data" / "airflow_task_instances.parquet"
    m = base / "artifacts" / "pipeline_failure_classifier.joblib"
    j = base / "artifacts" / "training_metrics.json"
    return (
        f"- Carpetas del paquete: `{base}`\n"
        f"- Export por defecto: `{h}`\n"
        f"- Modelo: `{m}` · métricas: `{j}`\n"
        "- Overrides opcionales en `.env`: `ML_HISTORY_PARQUET`, `ML_MODEL_PATH`, "
        "`ML_METRICS_PATH`\n"
        "- API Airflow: mismas variables que Pipeline Live (`AIRFLOW_API_BASE_URL`, "
        "`PIPELINE_DAG_ID`, credenciales)."
    )


def path_from_env(name: str, default: Path) -> Path:
    raw = os.getenv(name, "").strip()
    return Path(raw) if raw else default
