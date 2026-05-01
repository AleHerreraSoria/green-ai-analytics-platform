"""
Mapeo índice de etapa del pipeline → capa del data lake (bronze / silver / gold).
Los primeros 5 tasks son bronze, los siguientes 3 silver, los últimos 2 gold.
Debe coincidir con TASK_ORDER en airflow_state_client y medallas en pipeline_timeline.
"""

from __future__ import annotations

from typing import Literal

TierId = Literal["bronze", "silver", "gold"]

_BRONZE_END = 5
_SILVER_END = 8


def stage_index_to_tier(index: int) -> TierId:
    if index < _BRONZE_END:
        return "bronze"
    if index < _SILVER_END:
        return "silver"
    return "gold"
