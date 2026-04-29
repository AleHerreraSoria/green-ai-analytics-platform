"""
Inferencia de riesgo de fallo y anomalía de duración (último dag run).
Reutilizable desde Streamlit o CLI.
"""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd

_CT_ROOT = Path(__file__).resolve().parents[1]
if str(_CT_ROOT) not in sys.path:
    sys.path.insert(0, str(_CT_ROOT))

from utils.project_env import ensure_dotenv_loaded

from ML.airflow_client import AirflowRESTClient, config_from_env
from ML.export_airflow_metadata import fetch_latest_run_dataframe
from ML.features import FEATURE_COLUMNS, load_metrics, prepare_labeled_table
from ML.ml_package import ml_package_dir, path_from_env


def score_latest_run_dataframe(
    history_parquet: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    ti_page_size: int = 200,
) -> tuple[pd.DataFrame | None, str | None]:
    """
    Devuelve (tabla ordenada por p_fail, None) o (None, mensaje de error).
    """
    ensure_dotenv_loaded()
    base = ml_package_dir()
    history_parquet = path_from_env(
        "ML_HISTORY_PARQUET", history_parquet or (base / "data" / "airflow_task_instances.parquet")
    )
    model_path = path_from_env(
        "ML_MODEL_PATH", model_path or (base / "artifacts" / "pipeline_failure_classifier.joblib")
    )
    metrics_path = path_from_env(
        "ML_METRICS_PATH", metrics_path or (base / "artifacts" / "training_metrics.json")
    )

    if not history_parquet.is_file():
        return None, f"No existe el histórico exportado: {history_parquet}"
    if not model_path.is_file():
        return (
            None,
            f"No hay modelo entrenado en {model_path}. Ejecutá "
            "`python ML/train_pipeline_ml.py` en la instancia del Control Tower.",
        )

    cfg = config_from_env()
    if not cfg.base_url:
        return None, "Falta AIRFLOW_API_BASE_URL en `.env`."

    client = AirflowRESTClient(cfg)
    latest = fetch_latest_run_dataframe(client, cfg.dag_id, ti_page_size)
    if latest.empty:
        return None, "Airflow no devolvió dag runs (¿API o permisos?)."

    hist = pd.read_parquet(history_parquet)
    latest_id = str(latest.iloc[0]["dag_run_id"])
    combined = pd.concat([hist, latest], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["dag_run_id", "task_id", "try_number"], keep="last"
    )

    prepared = prepare_labeled_table(combined)
    rows = prepared[prepared["dag_run_id"] == latest_id]
    if rows.empty:
        return (
            None,
            "El último run no tiene tareas terminales con duración todavía "
            "(sigue en ejecución o sin datos).",
        )

    pipe = joblib.load(model_path)
    X = rows[FEATURE_COLUMNS]
    proba = pipe.predict_proba(X)[:, 1]

    meta = load_metrics(metrics_path)
    p95_map = meta.get("task_duration_p95_sec", {})

    out = rows[["task_id", "task_state", "duration_sec", "dag_run_id"]].copy()
    out["p_fail"] = proba

    def _dur_anomaly(row: pd.Series) -> bool:
        tid = str(row["task_id"])
        if tid not in p95_map or pd.isna(row["duration_sec"]):
            return False
        return float(row["duration_sec"]) > float(p95_map[tid])

    out["duration_anomaly"] = out.apply(_dur_anomaly, axis=1)
    out = out.sort_values("p_fail", ascending=False)
    return out, None


def score_latest_run_cli_print(
    history_parquet: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    ti_page_size: int = 200,
) -> int:
    """Salida CLI; código de salida 0 ok, 1 error."""
    df, err = score_latest_run_dataframe(
        history_parquet=history_parquet,
        model_path=model_path,
        metrics_path=metrics_path,
        ti_page_size=ti_page_size,
    )
    if err:
        print(err, file=sys.stderr)
        return 1
    assert df is not None
    latest_id = str(df.iloc[0]["dag_run_id"])
    print(f"dag_run_id={latest_id}")
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 120)
    print(df.to_string(index=False))
    return 0
