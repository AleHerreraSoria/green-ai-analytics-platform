"""
Cliente de estado para Airflow API v1.
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from utils.state_schema import (
    PipelineSnapshot,
    StageState,
    UX_DONE,
    UX_ERROR,
    UX_RETRY,
    UX_RUNNING,
    UX_WAITING,
    utc_now_iso,
)


TASK_ORDER = [
    ("sync_sources_bucket_to_bronze", "Sincronizando fuentes"),
    ("build_aws_ec2_pricing_reference", "Preparando referencia de costos"),
    ("generate_usage_logs", "Generando trazas de uso"),
    ("ingest_electricity_maps_api", "Cargando señales de red"),
    ("validate_bronze_quality_remote", "Validando capa Bronze"),
    ("transform_bronze_to_silver_remote", "Transformando a Silver"),
    ("validate_silver_quality_remote", "Validando capa Silver"),
    ("audit_silver_data_remote", "Auditando calidad Silver"),
    ("build_kimball_gold_layer_remote", "Construyendo capa Gold"),
    ("validate_gold_quality_remote", "Validando capa Gold"),
]

RUN_STATE_MAP = {
    "queued": UX_WAITING,
    "running": UX_RUNNING,
    "success": UX_DONE,
    "failed": UX_ERROR,
    "up_for_retry": UX_RETRY,
    "up_for_reschedule": UX_WAITING,
}

TASK_STATE_MAP = RUN_STATE_MAP | {
    "none": UX_WAITING,
    "scheduled": UX_WAITING,
}


@dataclass(slots=True)
class AirflowClientConfig:
    base_url: str
    dag_id: str
    username: str | None
    password: str | None
    token: str | None
    timeout_seconds: int = 10


class AirflowStateClient:
    def __init__(self, config: AirflowClientConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")

    @classmethod
    def from_env(cls) -> "AirflowStateClient":
        return cls(
            AirflowClientConfig(
                base_url=os.getenv("AIRFLOW_API_BASE_URL", "").strip(),
                dag_id=os.getenv("PIPELINE_DAG_ID", "ingesta_full_pipeline_v3_gold"),
                username=os.getenv("AIRFLOW_API_USERNAME"),
                password=os.getenv("AIRFLOW_API_PASSWORD"),
                token=os.getenv("AIRFLOW_API_TOKEN"),
                timeout_seconds=int(os.getenv("AIRFLOW_API_TIMEOUT_SECONDS", "10")),
            )
        )

    def _auth_header(self) -> dict[str, str]:
        if self.config.token:
            return {"Authorization": f"Bearer {self.config.token}"}
        if self.config.username and self.config.password:
            raw = f"{self.config.username}:{self.config.password}".encode("utf-8")
            encoded = base64.b64encode(raw).decode("ascii")
            return {"Authorization": f"Basic {encoded}"}
        return {}

    def _request_json(self, endpoint: str) -> dict[str, Any]:
        if not self.base_url:
            raise ValueError("AIRFLOW_API_BASE_URL no configurado.")
        url = f"{self.base_url}{endpoint}"
        headers = {"Accept": "application/json"} | self._auth_header()
        request = Request(url=url, headers=headers)
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"Airflow API respondió {exc.code} para {endpoint}.") from exc
        except URLError as exc:
            raise RuntimeError("No se pudo conectar con Airflow API.") from exc

    def _latest_run(self) -> dict[str, Any] | None:
        encoded_dag = quote(self.config.dag_id, safe="")
        payload = self._request_json(
            f"/api/v1/dags/{encoded_dag}/dagRuns?order_by=-start_date&limit=1"
        )
        runs = payload.get("dag_runs", [])
        return runs[0] if runs else None

    def _task_instances(self, run_id: str) -> dict[str, dict[str, Any]]:
        encoded_dag = quote(self.config.dag_id, safe="")
        encoded_run = quote(run_id, safe="")
        payload = self._request_json(
            f"/api/v1/dags/{encoded_dag}/dagRuns/{encoded_run}/taskInstances?limit=200"
        )
        return {ti["task_id"]: ti for ti in payload.get("task_instances", [])}

    @staticmethod
    def _friendly_state(raw_state: str | None, table: dict[str, str]) -> str:
        if not raw_state:
            return UX_WAITING
        return table.get(raw_state.lower(), UX_WAITING)

    @staticmethod
    def _progress_for(state: str) -> float:
        if state == UX_DONE:
            return 1.0
        if state == UX_RUNNING:
            return 0.6
        if state == UX_RETRY:
            return 0.35
        if state == UX_ERROR:
            return 0.0
        return 0.0

    def fetch_snapshot(self) -> PipelineSnapshot:
        latest_run = self._latest_run()
        if latest_run is None:
            stages = [
                StageState(stage_id=task_id, label=label, state=UX_WAITING)
                for task_id, label in TASK_ORDER
            ]
            return PipelineSnapshot(
                dag_id=self.config.dag_id,
                run_id=None,
                run_state=UX_WAITING,
                stages=stages,
                current_stage_index=0,
                overall_progress=0.0,
                last_updated_at=utc_now_iso(),
                message="Esperando la próxima ejecución del pipeline.",
            )

        run_id = latest_run.get("dag_run_id") or latest_run.get("run_id")
        run_state = self._friendly_state(latest_run.get("state"), RUN_STATE_MAP)
        task_map = self._task_instances(run_id=run_id)

        stages: list[StageState] = []
        current_idx = 0
        progress_acc = 0.0
        for index, (task_id, label) in enumerate(TASK_ORDER):
            ti = task_map.get(task_id, {})
            state = self._friendly_state(ti.get("state"), TASK_STATE_MAP)
            stage = StageState(
                stage_id=task_id,
                label=label,
                state=state,
                started_at=ti.get("start_date"),
                ended_at=ti.get("end_date"),
                progress_hint=self._progress_for(state),
            )
            stages.append(stage)
            progress_acc += stage.progress_hint
            if state in {UX_RUNNING, UX_RETRY, UX_ERROR}:
                current_idx = index

        if run_state == UX_DONE:
            current_idx = len(stages) - 1
            overall = 1.0
        else:
            overall = max(0.02, min(0.98, progress_acc / max(len(stages), 1)))

        started_at = latest_run.get("start_date")
        started_msg = ""
        if started_at:
            try:
                started_msg = datetime.fromisoformat(started_at.replace("Z", "+00:00")).strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
            except ValueError:
                started_msg = started_at

        message = (
            f"Ejecución {run_id} en curso."
            if run_state in {UX_RUNNING, UX_RETRY}
            else f"Última ejecución: {started_msg or 'sin fecha'}."
        )

        return PipelineSnapshot(
            dag_id=self.config.dag_id,
            run_id=run_id,
            run_state=run_state,
            stages=stages,
            current_stage_index=current_idx,
            overall_progress=overall,
            last_updated_at=utc_now_iso(),
            message=message,
        )
