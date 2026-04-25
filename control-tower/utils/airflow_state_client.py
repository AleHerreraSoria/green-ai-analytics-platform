"""
Cliente de estado para Airflow API v1.
"""

from __future__ import annotations

import base64
import json
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

from utils.project_env import ensure_dotenv_loaded
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
    ("sync_sources_bucket_to_bronze", "Ingestando Datos Historicos"),
    ("build_aws_ec2_pricing_reference", "Generando Dataset de Precios"),
    ("generate_usage_logs", "Generando Logs Sintéticos"),
    ("ingest_electricity_maps_api", "Ingestando Datos de API"),
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

DEFAULT_PIPELINE_DAG_ID = "green-ai-full-pipeline"


def resolve_pipeline_dag_id() -> str:
    raw = os.getenv("PIPELINE_DAG_ID")
    if raw is None:
        return DEFAULT_PIPELINE_DAG_ID
    stripped = raw.strip()
    return stripped if stripped else DEFAULT_PIPELINE_DAG_ID


@dataclass(slots=True)
class AirflowClientConfig:
    base_url: str
    dag_id: str
    username: str | None
    password: str | None
    token: str | None
    authorization_header: str | None  # cabecera Authorization completa; prioridad sobre token y user/pass
    timeout_seconds: int = 10


class AirflowStateClient:
    def __init__(self, config: AirflowClientConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        self._fab_lock = threading.Lock()
        self._fab_cookie_header: str | None = None
        self._fab_login_failed = False

    def _should_use_fab_session(self) -> bool:
        if os.getenv("AIRFLOW_API_DISABLE_FAB_SESSION", "").strip().lower() in {"1", "true", "yes"}:
            return False
        if self.config.authorization_header or self.config.token:
            return False
        return bool(self.config.username and self.config.password)

    def _perform_fab_login(self) -> str:
        """Obtiene cookie de sesión FAB (mismo flujo que el UI) para APIs con auth backend session."""
        from http.cookiejar import CookieJar

        login_url = f"{self.base_url}/login/"
        jar = CookieJar()
        opener = build_opener(HTTPCookieProcessor(jar))
        with opener.open(
            Request(login_url, headers={"Accept": "text/html"}),
            timeout=self.config.timeout_seconds,
        ) as get_resp:
            html = get_resp.read().decode("utf-8", errors="replace")
        m = re.search(r'id="csrf_token"[^>]+value="([^"]+)"', html) or re.search(
            r'name="csrf_token"[^>]+value="([^"]+)"', html
        )
        if not m:
            raise RuntimeError(
                "No se encontró csrf_token en /login/ de Airflow; "
                "¿AIRFLOW_API_BASE_URL apunta al webserver correcto?"
            )
        csrf = m.group(1)
        body = urlencode(
            {
                "csrf_token": csrf,
                "username": self.config.username or "",
                "password": self.config.password or "",
            }
        ).encode("utf-8")
        post_req = Request(
            login_url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/html",
                "Referer": login_url,
            },
        )
        with opener.open(post_req, timeout=self.config.timeout_seconds) as post_resp:
            post_resp.read()
            final_url = post_resp.geturl()
        path_lower = urlparse(final_url).path.lower()
        if "/login" in path_lower:
            raise RuntimeError(
                "Login en Airflow falló: usuario o contraseña incorrectos (formulario FAB)."
            )
        parts = [f"{c.name}={c.value}" for c in jar]
        if not parts:
            raise RuntimeError("Tras el login FAB, Airflow no devolvió cookies de sesión.")
        return "; ".join(parts)

    def _ensure_fab_session_cookie(self) -> None:
        if not self._should_use_fab_session():
            return
        if self._fab_cookie_header is not None:
            return
        if self._fab_login_failed:
            return
        with self._fab_lock:
            if self._fab_cookie_header is not None or self._fab_login_failed:
                return
            try:
                self._fab_cookie_header = self._perform_fab_login()
            except Exception:
                self._fab_login_failed = True
                raise

    @classmethod
    def from_env(cls) -> "AirflowStateClient":
        ensure_dotenv_loaded()
        auth_raw = os.getenv("AIRFLOW_API_AUTHORIZATION", "").strip() or None
        return cls(
            AirflowClientConfig(
                base_url=os.getenv("AIRFLOW_API_BASE_URL", "").strip(),
                dag_id=resolve_pipeline_dag_id(),
                username=os.getenv("AIRFLOW_API_USERNAME"),
                password=os.getenv("AIRFLOW_API_PASSWORD"),
                token=os.getenv("AIRFLOW_API_TOKEN"),
                authorization_header=auth_raw,
                timeout_seconds=int(os.getenv("AIRFLOW_API_TIMEOUT_SECONDS", "10")),
            )
        )

    def _auth_header(self) -> dict[str, str]:
        if self.config.authorization_header:
            value = self.config.authorization_header.strip()
            lower = value.lower()
            if lower.startswith("bearer ") or lower.startswith("basic "):
                return {"Authorization": value}
            return {"Authorization": f"Bearer {value}"}
        if self.config.token:
            return {"Authorization": f"Bearer {self.config.token}"}
        if self.config.username and self.config.password:
            raw = f"{self.config.username}:{self.config.password}".encode("utf-8")
            encoded = base64.b64encode(raw).decode("ascii")
            return {"Authorization": f"Basic {encoded}"}
        return {}

    def _json_request(
        self, endpoint: str, *, method: str = "GET", body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if not self.base_url:
            raise ValueError("AIRFLOW_API_BASE_URL no configurado.")
        self._ensure_fab_session_cookie()
        url = f"{self.base_url}{endpoint}"
        headers = {"Accept": "application/json"} | self._auth_header()
        if self._fab_cookie_header:
            headers["Cookie"] = self._fab_cookie_header
        payload = None if body is None else json.dumps(body).encode("utf-8")
        if payload is not None:
            headers["Content-Type"] = "application/json"
        request = Request(url=url, headers=headers, method=method, data=payload)
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = ""
            try:
                raw = exc.read().decode("utf-8", errors="replace").strip()
                if raw:
                    detail = f" Detalle: {raw[:800]}"
            except OSError:
                pass
            raise RuntimeError(
                f"Airflow API respondió {exc.code} para {endpoint}.{detail}"
            ) from exc
        except URLError as exc:
            raise RuntimeError("No se pudo conectar con Airflow API.") from exc

    def _request_json(self, endpoint: str) -> dict[str, Any]:
        return self._json_request(endpoint, method="GET", body=None)

    def trigger_dag_run(self, conf: dict[str, Any] | None = None) -> str:
        """
        Crea un dag run manual vía REST (equivalente a «Trigger DAG» en la UI).
        Requiere permisos de escritura sobre el DAG en Airflow.
        """
        encoded_dag = quote(self.config.dag_id, safe="")
        payload: dict[str, Any] = {}
        if conf:
            payload["conf"] = conf
        result = self._json_request(
            f"/api/v1/dags/{encoded_dag}/dagRuns",
            method="POST",
            body=payload,
        )
        run_id = result.get("dag_run_id") or result.get("run_id")
        if not run_id:
            raise RuntimeError(f"Airflow no devolvió dag_run_id en la respuesta: {result!r}")
        return str(run_id)

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
                run_started_at=None,
                run_ended_at=None,
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

        if run_state in {UX_RUNNING, UX_RETRY}:
            message = f"Ejecución {run_id} en curso."
        elif run_state == UX_DONE:
            message = f"Ejecución {run_id} completada."
        elif run_state == UX_ERROR:
            message = f"Ejecución {run_id} finalizó con incidencias."
        else:
            message = f"Última ejecución: {started_msg or 'sin fecha'}."

        run_started_at = latest_run.get("start_date")
        run_ended_at = latest_run.get("end_date")

        return PipelineSnapshot(
            dag_id=self.config.dag_id,
            run_id=run_id,
            run_state=run_state,
            stages=stages,
            current_stage_index=current_idx,
            overall_progress=overall,
            last_updated_at=utc_now_iso(),
            message=message,
            run_started_at=run_started_at,
            run_ended_at=run_ended_at,
        )
