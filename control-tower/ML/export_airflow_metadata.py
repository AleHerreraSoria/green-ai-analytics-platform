"""
Exporta dag runs y task instances desde Airflow REST API a Parquet (o CSV).

Variables de entorno (mismas que Control Tower):
  AIRFLOW_API_BASE_URL, PIPELINE_DAG_ID
  AIRFLOW_API_USERNAME / PASSWORD, o TOKEN, o AIRFLOW_API_AUTHORIZATION
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd

from ML.airflow_client import AirflowRESTClient, config_from_env


def _iter_dag_runs(client: AirflowRESTClient, dag_id: str, page_size: int):
    offset = 0
    encoded = quote(dag_id, safe="")
    while True:
        path = (
            f"/api/v1/dags/{encoded}/dagRuns"
            f"?limit={page_size}&offset={offset}&order_by=-logical_date"
        )
        payload = client.get_json(path)
        runs = payload.get("dag_runs", [])
        if not runs:
            break
        for r in runs:
            yield r
        if len(runs) < page_size:
            break
        offset += page_size


def _fetch_task_instances(
    client: AirflowRESTClient, dag_id: str, run_id: str, page_size: int
) -> list[dict]:
    encoded_dag = quote(dag_id, safe="")
    encoded_run = quote(run_id, safe="")
    offset = 0
    out: list[dict] = []
    while True:
        path = (
            f"/api/v1/dags/{encoded_dag}/dagRuns/{encoded_run}/taskInstances"
            f"?limit={page_size}&offset={offset}"
        )
        payload = client.get_json(path)
        tis = payload.get("task_instances", [])
        if not tis:
            break
        out.extend(tis)
        if len(tis) < page_size:
            break
        offset += page_size
    return out


def fetch_latest_run_dataframe(
    client: AirflowRESTClient, dag_id: str, ti_page: int
) -> pd.DataFrame:
    """Último dag run con todas sus task instances (mismo esquema que export masivo)."""
    encoded = quote(dag_id, safe="")
    payload = client.get_json(
        f"/api/v1/dags/{encoded}/dagRuns?order_by=-logical_date&limit=1"
    )
    runs = payload.get("dag_runs", [])
    if not runs:
        return pd.DataFrame()
    dr = runs[0]
    run_id = dr.get("dag_run_id") or dr.get("run_id")
    if not run_id:
        return pd.DataFrame()
    rows: list[dict] = []
    run_state = dr.get("state")
    logical_date = dr.get("logical_date")
    run_start = dr.get("start_date")
    run_end = dr.get("end_date")
    for ti in _fetch_task_instances(client, dag_id, str(run_id), ti_page):
        rows.append(
            {
                "dag_id": dag_id,
                "dag_run_id": str(run_id),
                "run_state": run_state,
                "logical_date": logical_date,
                "run_start_date": run_start,
                "run_end_date": run_end,
                "task_id": ti.get("task_id"),
                "task_state": (ti.get("state") or "").lower() if ti.get("state") else None,
                "try_number": ti.get("try_number", 1),
                "start_date": ti.get("start_date"),
                "end_date": ti.get("end_date"),
                "duration": ti.get("duration"),
            }
        )
    return pd.DataFrame(rows)


def export_to_dataframe(
    client: AirflowRESTClient, dag_id: str, max_runs: int | None, run_page: int, ti_page: int
) -> pd.DataFrame:
    rows: list[dict] = []
    n = 0
    for dr in _iter_dag_runs(client, dag_id, run_page):
        if max_runs is not None and n >= max_runs:
            break
        n += 1
        run_id = dr.get("dag_run_id") or dr.get("run_id")
        if not run_id:
            continue
        run_state = dr.get("state")
        logical_date = dr.get("logical_date")
        run_start = dr.get("start_date")
        run_end = dr.get("end_date")
        tis = _fetch_task_instances(client, dag_id, str(run_id), ti_page)
        for ti in tis:
            rows.append(
                {
                    "dag_id": dag_id,
                    "dag_run_id": str(run_id),
                    "run_state": run_state,
                    "logical_date": logical_date,
                    "run_start_date": run_start,
                    "run_end_date": run_end,
                    "task_id": ti.get("task_id"),
                    "task_state": (ti.get("state") or "").lower() if ti.get("state") else None,
                    "try_number": ti.get("try_number", 1),
                    "start_date": ti.get("start_date"),
                    "end_date": ti.get("end_date"),
                    "duration": ti.get("duration"),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta metadatos Airflow a Parquet/CSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "airflow_task_instances.parquet",
        help="Ruta de salida (.parquet o .csv).",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Límite de dag runs a volcar (por defecto: todos los accesibles vía API).",
    )
    parser.add_argument("--run-page-size", type=int, default=100)
    parser.add_argument("--ti-page-size", type=int, default=200)
    args = parser.parse_args()

    cfg = config_from_env()
    if not cfg.base_url:
        raise SystemExit("Falta AIRFLOW_API_BASE_URL.")
    client = AirflowRESTClient(cfg)
    df = export_to_dataframe(
        client, cfg.dag_id, args.max_runs, args.run_page_size, args.ti_page_size
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        raise SystemExit("No se obtuvieron filas: ¿hay ejecuciones del DAG o permisos API?")
    if args.output.suffix.lower() == ".csv":
        df.to_csv(args.output, index=False)
    else:
        df.to_parquet(args.output, index=False)
    print(f"Escrito {len(df)} filas en {args.output}")


if __name__ == "__main__":
    main()
