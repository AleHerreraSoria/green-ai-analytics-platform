"""Features para clasificación de fallos de tareas y detección de duración anómala."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ML.task_order import task_index

TERMINAL_TASK_STATES = frozenset(
    {"success", "failed", "skipped", "upstream_failed"}
)


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def compute_duration_seconds(row: pd.Series) -> float | None:
    dur = row.get("duration")
    if dur is not None and not (isinstance(dur, float) and math.isnan(dur)):
        try:
            sec = float(dur)
            if sec >= 0:
                return sec
        except (TypeError, ValueError):
            pass
    start = row.get("start_date")
    end = row.get("end_date")
    if pd.isna(start) or pd.isna(end) or start is None or end is None:
        return None
    try:
        s = pd.Timestamp(start)
        e = pd.Timestamp(end)
        return max(0.0, (e - s).total_seconds())
    except Exception:
        return None


def enrich_export(df: pd.DataFrame) -> pd.DataFrame:
    """Añade duración, tiempo calendario y orden de tarea."""
    if df.empty:
        return df
    out = df.copy()
    out["logical_dt"] = _parse_ts(out["logical_date"])
    if out["logical_dt"].isna().any() and "run_start_date" in out.columns:
        fix = out["logical_dt"].isna()
        out.loc[fix, "logical_dt"] = _parse_ts(out.loc[fix, "run_start_date"])
    out["start_dt"] = _parse_ts(out["start_date"])
    out["duration_sec"] = out.apply(compute_duration_seconds, axis=1)
    out["start_hour"] = out["start_dt"].dt.hour.fillna(-1).astype(int)
    out["start_dow"] = out["start_dt"].dt.dayofweek.fillna(-1).astype(int)
    if "try_number" not in out.columns:
        out["try_number"] = 1
    out["try_number"] = pd.to_numeric(out["try_number"], errors="coerce").fillna(1).astype(int)
    out["task_order_idx"] = out["task_id"].astype(str).map(task_index)
    out["task_failed"] = out["task_state"].astype(str).str.lower().isin({"failed"})
    out["task_terminal"] = out["task_state"].astype(str).str.lower().isin(TERMINAL_TASK_STATES)
    return out


def add_prior_duration_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mediana expandente por task_id (solo filas previas): sin usar la duración del mismo evento.
    Requiere columnas logical_dt, dag_run_id, task_order_idx, duration_sec.
    """
    out = df.copy()
    out = out.sort_values(["logical_dt", "dag_run_id", "task_order_idx", "task_id"]).reset_index(
        drop=True
    )

    def rolling_median_prior(series: pd.Series) -> pd.Series:
        return series.expanding(min_periods=1).median().shift(1)

    out["median_duration_same_task_prior"] = out.groupby("task_id", group_keys=False)[
        "duration_sec"
    ].transform(rolling_median_prior)
    global_med = out["duration_sec"].median()
    if pd.isna(global_med):
        global_med = 0.0
    out["median_duration_same_task_prior"] = out["median_duration_same_task_prior"].fillna(
        global_med
    )
    return out


def prepare_labeled_table(df: pd.DataFrame) -> pd.DataFrame:
    """Tabla lista para entrenar/evaluar: solo estados terminales con duración."""
    e = enrich_export(df)
    e = add_prior_duration_features(e)
    mask = e["task_terminal"] & e["duration_sec"].notna()
    e = e.loc[mask].copy()
    e["y_fail"] = e["task_failed"].astype(int)
    return e


FEATURE_COLUMNS = [
    "task_id",
    "start_hour",
    "start_dow",
    "try_number",
    "task_order_idx",
    "median_duration_same_task_prior",
]


def load_metrics(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def save_metrics(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def dag_run_ids_temporal_split(
    prepared: pd.DataFrame, test_fraction: float
) -> tuple[set[str], set[str]]:
    """Partición por dag_run_id según máximo logical_dt (última fracción = test)."""
    if prepared.empty:
        return set(), set()
    run_end = prepared.groupby("dag_run_id")["logical_dt"].max().sort_values()
    ids = run_end.index.tolist()
    cut = max(1, int(len(ids) * (1.0 - test_fraction)))
    train_ids = set(ids[:cut])
    test_ids = set(ids[cut:])
    if not test_ids and len(ids) > 1:
        train_ids = set(ids[:-1])
        test_ids = {ids[-1]}
    return train_ids, test_ids
