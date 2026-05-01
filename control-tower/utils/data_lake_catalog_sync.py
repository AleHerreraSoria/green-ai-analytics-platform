"""
Sincroniza el catálogo de archivos por capa cuando avanza el pipeline (Airflow snapshot).
"""

from __future__ import annotations

import streamlit as st

from utils.data_lake_tiers import stage_index_to_tier
from utils.s3_data_lake_listing import list_object_keys
from utils.state_schema import PipelineSnapshot


def _tracks_manual_run(snapshot: PipelineSnapshot, celebrate_rid: str | None) -> bool:
    return (
        celebrate_rid is not None
        and snapshot.run_id is not None
        and celebrate_rid == snapshot.run_id
    )


def _reset_bucket_panel_for_new_celebrate(celebrate_rid: str | None) -> None:
    prev = st.session_state.get("_bucket_panel_celebrate_rid")
    if celebrate_rid == prev:
        return
    # Nuevo run manual (id distinto y no nulo): catálogo en blanco.
    if celebrate_rid is not None:
        st.session_state["bucket_catalog"] = {"bronze": [], "silver": [], "gold": []}
        st.session_state["bucket_stage_prev"] = {}
        st.session_state["bucket_synced_run_id"] = None
        st.session_state["bucket_list_errors"] = {"bronze": None, "silver": None, "gold": None}
    st.session_state["_bucket_panel_celebrate_rid"] = celebrate_rid


def _refresh_tier(tier: str) -> None:
    keys, err = list_object_keys(tier)
    st.session_state.setdefault("bucket_catalog", {"bronze": [], "silver": [], "gold": []})
    st.session_state.setdefault("bucket_list_errors", {"bronze": None, "silver": None, "gold": None})
    st.session_state["bucket_catalog"][tier] = keys
    st.session_state["bucket_list_errors"][tier] = err


def sync_data_lake_catalog_from_snapshot(
    snapshot: PipelineSnapshot,
    celebrate_rid: str | None,
) -> None:
    """
    Cuando una etapa pasa a completada, lista el bucket de la capa correspondiente.
    También hace catch-up al detectar el run manual en Airflow.
    """
    _reset_bucket_panel_for_new_celebrate(celebrate_rid)

    if celebrate_rid is None:
        return

    if not _tracks_manual_run(snapshot, celebrate_rid):
        return

    if snapshot.run_id != celebrate_rid:
        return

    # Primera vez que este run_id está visible en Airflow: refrescar capas ya terminadas.
    if st.session_state.get("bucket_synced_run_id") != snapshot.run_id:
        st.session_state["bucket_synced_run_id"] = snapshot.run_id
        tiers_to_refresh: set[str] = set()
        for idx, stg in enumerate(snapshot.stages):
            if stg.state == "done":
                tiers_to_refresh.add(stage_index_to_tier(idx))
        for tier in sorted(tiers_to_refresh):
            _refresh_tier(tier)
        st.session_state["bucket_stage_prev"] = {s.stage_id: s.state for s in snapshot.stages}
        return

    prev: dict[str, str] = st.session_state.setdefault("bucket_stage_prev", {})
    tiers_touch: set[str] = set()

    for idx, stg in enumerate(snapshot.stages):
        old = prev.get(stg.stage_id)
        new = stg.state
        if old != "done" and new == "done":
            tiers_touch.add(stage_index_to_tier(idx))
        prev[stg.stage_id] = new

    for tier in sorted(tiers_touch):
        _refresh_tier(tier)
