"""
Vista live del pipeline E2E: experiencia visual de avance en tiempo real.
"""

from __future__ import annotations

import os

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from components.pipeline_timeline import render_pipeline_timeline
from utils.airflow_state_client import AirflowStateClient
from utils.realtime_stream import RealtimePipelineStream


st.set_page_config(page_title="Pipeline Live", page_icon="🚀", layout="wide")

st.title("Pipeline Live Tracker")
st.caption(
    "Seguimiento visual del flujo Airflow sin logs técnicos: progreso, etapa actual y estado general."
)

airflow_base_url = os.getenv("AIRFLOW_API_BASE_URL", "").strip()
if not airflow_base_url:
    st.warning(
        "Configura `AIRFLOW_API_BASE_URL` para habilitar el seguimiento en tiempo real."
    )
    st.stop()

poll_interval = int(os.getenv("AIRFLOW_POLL_INTERVAL_SECONDS", "6"))
heartbeat_seconds = int(os.getenv("STREAM_HEARTBEAT_SECONDS", "2"))
refresh_ms = max(2000, heartbeat_seconds * 1000)

if "pipeline_stream" not in st.session_state:
    client = AirflowStateClient.from_env()
    stream = RealtimePipelineStream(
        client=client,
        poll_interval=poll_interval,
        heartbeat_seconds=heartbeat_seconds,
    )
    stream.start()
    st.session_state.pipeline_stream = stream
    st.session_state.stream_events = []

stream: RealtimePipelineStream = st.session_state.pipeline_stream
events = stream.consume_events(limit=30)
if events:
    st.session_state.stream_events.extend(events)
    st.session_state.stream_events = st.session_state.stream_events[-60:]

snapshot = stream.latest_snapshot()
if snapshot is None:
    st.info("Iniciando conexión con Airflow...")
else:
    render_pipeline_timeline(snapshot)

    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.metric("Progreso global", f"{int(snapshot.overall_progress * 100)}%")
    with mc2:
        live_mode = "Push" if snapshot.source_mode == "push" else "Fallback polling"
        st.metric("Modo de actualización", live_mode)
    with mc3:
        st.metric("Última actualización", snapshot.last_updated_at[:19].replace("T", " "))

last_error = stream.latest_error()
if last_error:
    st.warning(
        "Se detectó una reconexión temporal. "
        "El tablero sigue activo con fallback automático."
    )

with st.expander("Actividad del stream", expanded=False):
    if not st.session_state.stream_events:
        st.caption("Aún no hay eventos.")
    for event in reversed(st.session_state.stream_events[-10:]):
        st.write(f"- `{event.created_at}` · `{event.event_type}`")

st_autorefresh(interval=refresh_ms, key="pipeline-live-refresh")
