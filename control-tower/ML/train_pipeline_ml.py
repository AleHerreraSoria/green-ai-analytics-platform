"""
Entrena un clasificador (RandomForest) para P(fallo de tarea | features previas).

Si el train no tiene fallos y éxitos, se guarda un modelo degenerado (probabilidad constante)
para no bloquear el flujo; conviene reentrenar cuando haya ambas clases.

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

from ML.degenerate_classifier import DegenerateFailureClassifier
from ML.features import (
    FEATURE_COLUMNS,
    dag_run_ids_temporal_split,
    ensure_non_empty_test_if_possible,
    prepare_labeled_table,
    rebalance_train_for_both_classes,
    save_metrics,
)


def _build_preprocess() -> ColumnTransformer:
    return ColumnTransformer(
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


def _build_rf_pipeline() -> Pipeline:
    clf = RandomForestClassifier(
        n_estimators=120,
        max_depth=12,
        min_samples_leaf=2,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )
    return Pipeline([("prep", _build_preprocess()), ("clf", clf)])


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

    train_ids, test_ids = dag_run_ids_temporal_split(prepared, args.test_fraction)
    split_notes: list[str] = []

    train_ids, test_ids, reb_adj = rebalance_train_for_both_classes(
        prepared, train_ids, test_ids
    )
    if reb_adj:
        split_notes.append(
            "Split ajustado (demo/pocos runs): se movieron dag runs del test al train para "
            "que el entrenamiento tenga tanto éxitos como fallos cuando el histórico global los tiene."
        )

    train_ids, test_ids, test_adj = ensure_non_empty_test_if_possible(
        prepared, train_ids, test_ids
    )
    if test_adj:
        split_notes.append(
            "Test repoblado: un dag run antiguo pasó al conjunto de test para poder medir sin "
            "dejar el train sin ambas clases."
        )

    _dr = prepared["dag_run_id"].astype(str)
    train_df = prepared.loc[_dr.isin(train_ids)].copy()
    test_df = prepared.loc[_dr.isin(test_ids)].copy()
    if train_df.empty:
        raise SystemExit("Train vacío tras el split; acumula más dag runs o reduce --test-fraction.")

    eval_on_train_only = test_df.empty
    if eval_on_train_only:
        split_notes.append(
            "Solo un dag run útil o test vacío: las métricas se calculan sobre el mismo train "
            "(solo demo; no uses estas cifras como evaluación honesta)."
        )

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df["y_fail"]
    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df["y_fail"]

    if eval_on_train_only:
        X_eval, y_eval = X_train, y_train
    else:
        X_eval, y_eval = X_test, y_test

    train_has_both_classes = y_train.nunique() >= 2
    if train_has_both_classes:
        pipe = _build_rf_pipeline()
        pipe.fit(X_train, y_train)
        proba = pipe.predict_proba(X_eval)[:, 1]
        report = classification_report(y_eval, (proba >= 0.5).astype(int), zero_division=0)
        auc = (
            float(roc_auc_score(y_eval, proba)) if y_eval.nunique() > 1 else None
        )
        degenerate_note = None
    else:
        pipe = Pipeline([("prep", _build_preprocess()), ("clf", DegenerateFailureClassifier())])
        pipe.fit(X_train, y_train)
        proba = pipe.predict_proba(X_eval)[:, 1]
        report = classification_report(y_eval, (proba >= 0.5).astype(int), zero_division=0)
        auc = None
        if prepared["y_fail"].nunique() < 2:
            degenerate_note = (
                "El histórico solo tiene una clase (solo éxitos o solo fallos). "
                "P(fallo) es constante; cuando haya ambas clases, reentrená para un modelo útil."
            )
        else:
            degenerate_note = (
                "En el tramo temporal de entrenamiento solo hay una clase; "
                "P(fallo) es constante en este modelo."
            )

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
        "degenerate_single_class_train": not train_has_both_classes,
        "evaluated_on_train_only": eval_on_train_only,
    }
    if split_notes:
        payload["split_notes"] = split_notes
    if degenerate_note:
        payload["degenerate_note"] = degenerate_note
    save_metrics(args.artifacts_dir / "training_metrics.json", payload)

    print(report)
    if degenerate_note:
        print(degenerate_note)
    if auc is not None:
        print(f"ROC-AUC (test): {auc:.4f}")
    print(f"Modelo guardado en {model_path}")


if __name__ == "__main__":
    main()
