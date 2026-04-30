"""
Inferencia de riesgo de fallo y anomalía de duración (último dag run).
Reutilizable desde Streamlit o CLI.
"""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

_CT_ROOT = Path(__file__).resolve().parents[1]
if str(_CT_ROOT) not in sys.path:
    sys.path.insert(0, str(_CT_ROOT))

from utils.project_env import ensure_dotenv_loaded

from ML.airflow_client import AirflowRESTClient, config_from_env
import ML.degenerate_classifier  # noqa: F401 — joblib puede cargar DegenerateFailureClassifier
from ML.export_airflow_metadata import fetch_latest_run_dataframe
from ML.features import (
    enrich_for_model_inference,
    load_metrics,
    sanitize_features_for_predict,
)
from ML.ml_package import ml_package_dir, path_from_env


def score_latest_run_dataframe(
    history_parquet: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    ti_page_size: int = 200,
) -> tuple[pd.DataFrame | None, str | None]:
    """
    Devuelve (tabla ordenada por p_fail, None) o (None, mensaje de error).
    P(fallo) se calcula para **todas** las tareas del último run, imputando features
    si hace falta (misma base que entrenamiento).
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
    combined["dag_run_id"] = combined["dag_run_id"].astype(str)
    combined = combined.drop_duplicates(
        subset=["dag_run_id", "task_id", "try_number"], keep="last"
    )

    enriched = enrich_for_model_inference(combined)
    latest_rows = enriched[enriched["dag_run_id"].astype(str) == latest_id].copy()
    if latest_rows.empty:
        return None, "No hay task instances del último dag run en los datos combinados."

    try:
        pipe = joblib.load(model_path)
    except AttributeError as exc:
        err_low = str(exc).lower()
        if "degeneratefailureclassifier" in err_low.replace("_", "") or "__main__" in str(exc):
            return (
                None,
                "El archivo del modelo es incompatible con esta versión (pickle antiguo). "
                "Pulsá «Actualizar histórico y reentrenar modelo» para volver a generar el joblib.",
            )
        raise

    X = sanitize_features_for_predict(latest_rows)
    proba = np.asarray(pipe.predict_proba(X)[:, 1], dtype=float)
    proba = np.nan_to_num(proba, nan=0.0, posinf=0.0, neginf=0.0)

    meta = load_metrics(metrics_path)
    p95_map = meta.get("task_duration_p95_sec", {})

    latest_rows = latest_rows.copy()
    latest_rows["p_fail"] = proba
    latest_rows = latest_rows.sort_values(
        ["task_order_idx", "p_fail"],
        ascending=[True, False],
    )

    out = latest_rows[["task_id", "task_state", "duration_sec", "dag_run_id", "p_fail"]].copy()
    out["task_state"] = (
        out["task_state"].fillna("—").astype(str).replace({"nan": "—", "None": "—", "<NA>": "—"})
    )

    def _dur_anomaly(row: pd.Series) -> bool:
        tid = str(row["task_id"])
        ds = row["duration_sec"]
        if tid not in p95_map or pd.isna(ds):
            return False
        return float(ds) > float(p95_map[tid])

    out["duration_anomaly"] = out.apply(_dur_anomaly, axis=1)
    out["duration_sec"] = pd.to_numeric(out["duration_sec"], errors="coerce").fillna(-1.0)
    out["p_fail"] = np.clip(np.asarray(out["p_fail"], dtype=float), 0.0, 1.0)
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
