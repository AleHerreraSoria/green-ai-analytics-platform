"""
Entrena un clasificador simple (RandomForest) para P(fallo de tarea | features previas).

Requiere export previo: python ML/export_airflow_metadata.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from ML.features import (
    FEATURE_COLUMNS,
    dag_run_ids_temporal_split,
    prepare_labeled_table,
    save_metrics,
)


def _build_pipeline() -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                ["task_id"],
            ),
            (
                "num",
                "passthrough",
                [
                    "start_hour",
                    "start_dow",
                    "try_number",
                    "task_order_idx",
                    "median_duration_same_task_prior",
                ],
            ),
        ]
    )
    clf = RandomForestClassifier(
        n_estimators=120,
        max_depth=12,
        min_samples_leaf=2,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )
    return Pipeline([("prep", preprocess), ("clf", clf)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Entrena modelo de riesgo de fallo de tarea.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "airflow_task_instances.parquet",
        help="Parquet exportado desde Airflow.",
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=0.2,
        help="Fracción temporal de dag runs reservada para test.",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "artifacts",
    )
    args = parser.parse_args()

    if not args.data.is_file():
        raise SystemExit(f"No existe el dataset: {args.data}. Ejecuta primero export_airflow_metadata.py.")

    raw = pd.read_parquet(args.data)
    prepared = prepare_labeled_table(raw)
    if prepared.empty:
        raise SystemExit("Sin filas etiquetadas (¿hay runs terminados con duración?).")
    if prepared["y_fail"].nunique() < 2:
        raise SystemExit(
            "Solo hay una clase en y_fail; se necesitan ejemplos de fallo y éxito para entrenar."
        )

    train_ids, test_ids = dag_run_ids_temporal_split(prepared, args.test_fraction)
    train_df = prepared[prepared["dag_run_id"].isin(train_ids)].copy()
    test_df = prepared[prepared["dag_run_id"].isin(test_ids)].copy()
    if train_df.empty or test_df.empty:
        raise SystemExit("Split temporal vacío; acumula más dag runs o reduce --test-fraction.")

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df["y_fail"]
    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df["y_fail"]

    pipe = _build_pipeline()
    pipe.fit(X_train, y_train)

    proba = pipe.predict_proba(X_test)[:, 1]
    report = classification_report(y_test, (proba >= 0.5).astype(int), zero_division=0)
    auc = float(roc_auc_score(y_test, proba)) if y_test.nunique() > 1 else None

    args.artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.artifacts_dir / "pipeline_failure_classifier.joblib"
    joblib.dump(pipe, model_path)

    duration_med = train_df.groupby("task_id")["duration_sec"].median().to_dict()
    duration_p95 = (
        train_df.groupby("task_id")["duration_sec"].quantile(0.95).fillna(0).to_dict()
    )
    payload = {
        "model_path": str(model_path),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "dag_train_runs": len(train_ids),
        "dag_test_runs": len(test_ids),
        "roc_auc": auc,
        "classification_report": report,
        "task_duration_median_sec": {str(k): float(v) for k, v in duration_med.items()},
        "task_duration_p95_sec": {str(k): float(v) for k, v in duration_p95.items()},
    }
    save_metrics(args.artifacts_dir / "training_metrics.json", payload)

    print(report)
    if auc is not None:
        print(f"ROC-AUC (test): {auc:.4f}")
    print(f"Modelo guardado en {model_path}")


if __name__ == "__main__":
    main()
