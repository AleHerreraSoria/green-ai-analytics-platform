"""
Contrato canónico de estado para el pipeline visual live.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


UX_WAITING = "waiting"
UX_RUNNING = "running"
UX_DONE = "done"
UX_ERROR = "error"
UX_RETRY = "retry"

VALID_UX_STATES = {UX_WAITING, UX_RUNNING, UX_DONE, UX_ERROR, UX_RETRY}


@dataclass(slots=True)
class StageState:
    stage_id: str
    label: str
    state: str
    started_at: str | None = None
    ended_at: str | None = None
    progress_hint: float = 0.0
    compute_details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.compute_details is None:
            data.pop("compute_details")
        return data


@dataclass(slots=True)
class PipelineSnapshot:
    dag_id: str
    run_id: str | None
    run_state: str
    stages: list[StageState]
    current_stage_index: int
    overall_progress: float
    last_updated_at: str
    source_mode: str = "push"
    message: str | None = None
    run_started_at: str | None = None
    run_ended_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "run_state": self.run_state,
            "stages": [stage.to_dict() for stage in self.stages],
            "current_stage_index": self.current_stage_index,
            "overall_progress": self.overall_progress,
            "last_updated_at": self.last_updated_at,
            "source_mode": self.source_mode,
            "message": self.message,
            "run_started_at": self.run_started_at,
            "run_ended_at": self.run_ended_at,
        }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
