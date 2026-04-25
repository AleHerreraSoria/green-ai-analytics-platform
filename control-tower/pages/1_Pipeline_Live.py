"""
Vista live del pipeline E2E: experiencia visual de avance en tiempo real.
"""

from __future__ import annotations

import html
import os

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.project_env import ensure_dotenv_loaded

ensure_dotenv_loaded()

from components.pipeline_timeline import (
    pipeline_timer_metric,
    pipeline_tracks_manual_run,
    render_pipeline_timeline,
)
from utils.airflow_state_client import AirflowStateClient, resolve_pipeline_dag_id
from utils.realtime_stream import RealtimePipelineStream


def _pipeline_metric_cell(label: str, value: str) -> str:
    """Misma lectura que st.metric, con título y valor centrados (evita el grid interno de Streamlit)."""
    return (
        '<div class="ct-metric-cell">'
        f'<div class="ct-metric-l">{html.escape(label)}</div>'
        f'<div class="ct-metric-v">{html.escape(str(value))}</div>'
        "</div>"
    )


st.set_page_config(page_title="Pipeline Live", page_icon="🚀", layout="wide")

st.title("Pipeline Live Tracker")

airflow_base_url = os.getenv("AIRFLOW_API_BASE_URL", "").strip()
if not airflow_base_url:
    st.warning(
        "Configura `AIRFLOW_API_BASE_URL` para habilitar el seguimiento en tiempo real."
    )
    st.stop()

poll_interval = int(os.getenv("AIRFLOW_POLL_INTERVAL_SECONDS", "6"))
heartbeat_seconds = int(os.getenv("STREAM_HEARTBEAT_SECONDS", "2"))
refresh_ms = max(2000, heartbeat_seconds * 1000)
manual_trigger_enabled = os.getenv("PIPELINE_ALLOW_MANUAL_TRIGGER", "").strip().lower() in {
    "1",
    "true",
    "yes",
}

if "pipeline_stream" in st.session_state:
    if st.session_state.pipeline_stream.client.config.dag_id != resolve_pipeline_dag_id():
        st.session_state.pipeline_stream.stop()
        for _k in ("pipeline_stream", "stream_events", "pipeline_celebrate_run_id"):
            st.session_state.pop(_k, None)

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

if manual_trigger_enabled:
    st.markdown(
        """
        <style>
        /* Único botón de esta página: fondo negro fijo, texto lima; hover solo cambia color del texto */
        section[data-testid="stMain"] button[kind="tertiary"] {
            min-height: 5.15rem !important;
            padding-top: 0.72rem !important;
            padding-bottom: 0.72rem !important;
            background: #0a0a0a !important;
            background-color: #0a0a0a !important;
            border: 1px solid rgba(222, 255, 154, 0.32) !important;
            box-shadow: none !important;
            color: #deff9a !important;
        }
        section[data-testid="stMain"] button[kind="tertiary"] p {
            font-size: 2.52rem !important;
            font-weight: 600 !important;
            line-height: 1.22 !important;
            color: inherit !important;
        }
        section[data-testid="stMain"] button[kind="tertiary"]:hover,
        section[data-testid="stMain"] button[kind="tertiary"]:focus-visible {
            background: #0a0a0a !important;
            background-color: #0a0a0a !important;
            border-color: rgba(158, 220, 104, 0.55) !important;
            color: #9edc68 !important;
            box-shadow: none !important;
        }
        section[data-testid="stMain"] button[kind="tertiary"]:hover p,
        section[data-testid="stMain"] button[kind="tertiary"]:focus-visible p {
            color: #9edc68 !important;
        }
        section[data-testid="stMain"] button[kind="tertiary"]:active {
            background-color: #0a0a0a !important;
            color: #b8f070 !important;
        }
        section[data-testid="stMain"] button[kind="tertiary"]:active p {
            color: #b8f070 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    _, btn_col = st.columns([4.45, 1.25])
    with btn_col:
        if st.button(
            "▶️ Ejecutar Pipeline",
            key="trigger_pipeline_dag",
            type="tertiary",
            use_container_width=True,
        ):
            try:
                run_id = st.session_state.pipeline_stream.client.trigger_dag_run()
                st.session_state["pipeline_celebrate_run_id"] = run_id
                st.toast(f"Ejecución iniciada: {run_id}", icon="✅")
                st.success(
                    f"Se disparó un nuevo run (`{run_id}`). El timeline se actualizará cuando Airflow lo tome."
                )
            except RuntimeError as exc:
                st.error(str(exc))

stream: RealtimePipelineStream = st.session_state.pipeline_stream
events = stream.consume_events(limit=30)
if events:
    st.session_state.stream_events.extend(events)
    st.session_state.stream_events = st.session_state.stream_events[-60:]

snapshot = stream.latest_snapshot()
if snapshot is None:
    st.info("Iniciando conexión con Airflow...")
else:
    celebrate_rid = st.session_state.get("pipeline_celebrate_run_id")
    tracks_manual = pipeline_tracks_manual_run(snapshot, celebrate_rid)
    show_completion_hero = (
        snapshot.run_state == "done"
        and snapshot.run_id is not None
        and celebrate_rid is not None
        and celebrate_rid == snapshot.run_id
    )
    render_pipeline_timeline(
        snapshot,
        show_completion_hero=show_completion_hero,
        tracked_manual_run_id=celebrate_rid,
    )

    progress_pct = int(snapshot.overall_progress * 100) if tracks_manual else 0
    time_label, time_value = pipeline_timer_metric(snapshot, celebrate_rid)
    live_mode = "Push" if snapshot.source_mode == "push" else "Fallback polling"
    last_upd = snapshot.last_updated_at[:19].replace("T", " ")

    st.markdown(
        """
        <style>
        /* Celdas tipo métrica (HTML propio): título y valor siempre centrados */
        div[data-testid="stMarkdownContainer"] .ct-metric-cell {
            text-align: center;
            width: 100%;
            padding: 0.1rem 0 0.25rem 0;
        }
        div[data-testid="stMarkdownContainer"] .ct-metric-l {
            font-size: 0.875rem;
            line-height: 1.35;
            color: rgba(250, 250, 250, 0.72);
            font-weight: 400;
        }
        div[data-testid="stMarkdownContainer"] .ct-metric-v {
            font-size: clamp(1.35rem, 3.2vw, 1.85rem);
            font-weight: 600;
            line-height: 1.15;
            color: rgb(250, 250, 250);
            margin-top: 0.12rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    mc_last, mc_mode, mc_time, mc_prog = st.columns(4)
    with mc_last:
        st.markdown(_pipeline_metric_cell("Última actualización", last_upd), unsafe_allow_html=True)
    with mc_mode:
        st.markdown(_pipeline_metric_cell("Modo de actualización", live_mode), unsafe_allow_html=True)
    with mc_time:
        st.markdown(_pipeline_metric_cell(time_label, time_value), unsafe_allow_html=True)
    with mc_prog:
        st.markdown(_pipeline_metric_cell("Progreso global", f"{progress_pct}%"), unsafe_allow_html=True)

last_error = stream.latest_error()
if last_error:
    st.warning(
        "No se pudo obtener el estado desde la API de Airflow; el hilo de fondo sigue reintentando."
    )
    st.code(last_error, language="text")
    if "401" in last_error:
        st.info(
            "**401 — no autorizado**\n\n"
            "- Comprobá usuario y contraseña en `.env` (mismos que el login del UI de **esta** instancia).\n"
            "- En Airflow suele faltar **Basic** en la API: en el servidor, "
            "`AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth` "
            "(los dos, en ese orden, es un valor habitual cuando el UI usa sesión y querés API con usuario/contraseña).\n"
            "- Si ya usás Bearer en el navegador (DevTools → cabecera `Authorization` de una petición a `/api/v1/…`), "
            "podés copiarla en `AIRFLOW_API_AUTHORIZATION` en `.env` (valor completo tras `Authorization: `).\n"
            "- Alternativa: `AIRFLOW_API_TOKEN` si tu equipo entrega un JWT/API key para la API.\n"
            "- Tras cambiar `.env`, reiniciá el proceso de Streamlit (o cerrá la pestaña) para que se vuelva a crear el cliente."
        )

with st.expander("Actividad del stream", expanded=False):
    if not st.session_state.stream_events:
        st.caption("Aún no hay eventos.")
    for event in reversed(st.session_state.stream_events[-10:]):
        st.write(f"- `{event.created_at}` · `{event.event_type}`")

st_autorefresh(interval=refresh_ms, key="pipeline-live-refresh")
