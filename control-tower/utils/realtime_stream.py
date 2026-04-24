"""
Canal de eventos live para Control Tower (SSE-like interno sobre polling).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Event, Lock, Thread
from time import sleep

from utils.airflow_state_client import AirflowStateClient
from utils.state_schema import PipelineSnapshot, utc_now_iso


@dataclass(slots=True)
class StreamEvent:
    event_type: str
    payload: dict
    created_at: str


class RealtimePipelineStream:
    def __init__(self, client: AirflowStateClient, poll_interval: int = 6, heartbeat_seconds: int = 2):
        self.client = client
        self.poll_interval = max(2, poll_interval)
        self.heartbeat_seconds = max(1, heartbeat_seconds)
        self._buffer: deque[StreamEvent] = deque(maxlen=100)
        self._latest_snapshot: PipelineSnapshot | None = None
        self._last_stage_idx: int | None = None
        self._mode = "push"
        self._lock = Lock()
        self._stop = Event()
        self._worker: Thread | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = Thread(target=self._run, daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()
        if self._worker:
            self._worker.join(timeout=1.5)

    def _push(self, event_type: str, payload: dict) -> None:
        with self._lock:
            self._buffer.append(
                StreamEvent(event_type=event_type, payload=payload, created_at=utc_now_iso())
            )

    def _run(self) -> None:
        tick = 0
        while not self._stop.is_set():
            tick += 1
            try:
                snapshot = self.client.fetch_snapshot()
                snapshot.source_mode = self._mode
                with self._lock:
                    self._latest_snapshot = snapshot
                self._push("pipeline_snapshot", snapshot.to_dict())

                if self._last_stage_idx is not None and snapshot.current_stage_index != self._last_stage_idx:
                    self._push(
                        "stage_transition",
                        {
                            "from_stage_index": self._last_stage_idx,
                            "to_stage_index": snapshot.current_stage_index,
                            "run_id": snapshot.run_id,
                        },
                    )
                self._last_stage_idx = snapshot.current_stage_index

                if snapshot.run_state in {"done", "error"}:
                    self._push(
                        "run_finished",
                        {"run_id": snapshot.run_id, "run_state": snapshot.run_state},
                    )
                self._last_error = None
                sleep(self.poll_interval)
            except Exception as exc:  # noqa: BLE001 - queremos resiliencia en stream
                self._last_error = str(exc)
                self._mode = "polling"
                self._push(
                    "stream_fallback",
                    {
                        "mode": "polling",
                        "reason": "No hubo conexión estable al canal push; se activa fallback.",
                    },
                )
                sleep(self.heartbeat_seconds)
            finally:
                if tick % max(1, self.heartbeat_seconds) == 0:
                    self._push("heartbeat", {"mode": self._mode})

    def consume_events(self, limit: int = 20) -> list[StreamEvent]:
        events: list[StreamEvent] = []
        with self._lock:
            while self._buffer and len(events) < limit:
                events.append(self._buffer.popleft())
        return events

    def latest_snapshot(self) -> PipelineSnapshot | None:
        with self._lock:
            return self._latest_snapshot

    def latest_error(self) -> str | None:
        return self._last_error
