"""
Vista live del pipeline E2E: experiencia visual de avance en tiempo real.
"""

from __future__ import annotations

import html
import os
import time

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.project_env import ensure_dotenv_loaded

ensure_dotenv_loaded()

from components.pipeline_live_combined import render_pipeline_live_stack
from components.pipeline_timeline import (
    pipeline_timer_metric,
    pipeline_tracks_manual_run,
)
from utils.airflow_state_client import AirflowStateClient, resolve_pipeline_dag_id
from utils.data_lake_catalog_sync import sync_data_lake_catalog_from_snapshot
from utils.realtime_stream import RealtimePipelineStream
from utils.state_schema import PipelineSnapshot, StageState

COMPLETION_RESET_DELAY_SECONDS = 20


def _idle_pipeline_snapshot(snapshot: PipelineSnapshot, message: str) -> PipelineSnapshot:
    """Sin run propio en sesión: no mostrar el último estado de Airflow como si fuera la corrida actual."""
    return PipelineSnapshot(
        dag_id=snapshot.dag_id,
        run_id=None,
        run_state="waiting",
        stages=[
            StageState(stage_id=s.stage_id, label=s.label, state="waiting")
            for s in snapshot.stages
        ],
        current_stage_index=0,
        overall_progress=0.0,
        last_updated_at=snapshot.last_updated_at,
        source_mode=snapshot.source_mode,
        message=message,
        run_started_at=None,
        run_ended_at=None,
    )


def _starting_pipeline_snapshot(snapshot: PipelineSnapshot) -> PipelineSnapshot:
    """Ya disparaste el DAG; Airflow aún no expone ese run como último — evita mostrar un run viejo en verde."""
    return PipelineSnapshot(
        dag_id=snapshot.dag_id,
        run_id=None,
        run_state="running",
        stages=[
            StageState(stage_id=s.stage_id, label=s.label, state="waiting")
            for s in snapshot.stages
        ],
        current_stage_index=0,
        overall_progress=0.0,
        last_updated_at=snapshot.last_updated_at,
        source_mode=snapshot.source_mode,
        message="Iniciando ejecución…",
        run_started_at=None,
        run_ended_at=None,
    )


def _pipeline_metric_cell_html(label: str, value: str) -> str:
    """Métricas grandes y centradas; usa `st.html` para que Streamlit no aplique tipografía Markdown encima."""
    lab = html.escape(label)
    val = html.escape(str(value))
    return (
        '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;'
        'text-align:center;width:100%;padding:0.35rem 0.5rem;box-sizing:border-box;">'
        '<div style="font-size:clamp(14px,1.9vw,22px);line-height:1.2;color:rgba(250,250,250,0.85);'
        'font-weight:600;font-family:system-ui,-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;">'
        f"{lab}</div>"
        '<div style="font-size:clamp(28px,3.8vw,44px);line-height:1.08;color:rgb(250,250,250);'
        'font-weight:700;margin-top:0.4rem;font-variant-numeric:tabular-nums;'
        'font-family:system-ui,-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;">'
        f"{val}</div>"
        "</div>"
    )


st.set_page_config(page_title="Pipeline Live", page_icon="🚀", layout="wide")


def _render_pipeline_request_ack_float() -> None:
    """Aviso tipo ventana sin usar `st.dialog` (evita el overlay oscuro del modal de Streamlit)."""
    # `width="content"` evita que el contenedor estire todo el ancho mientras está en flujo normal.
    with st.container(width="content", key="pipeline_ack_float"):
        st.markdown(
            """
            <style>
            span.ct-pipeline-ack-float-anchor { display: none !important; }
            /*
              IMPORTANTE: no usar solo `:has(...)` sobre un único `stVerticalBlock`: el bloque
              principal de la página también contiene el span como descendiente y acababa
              aplicando `width: min(460px)` a TODA la vista. Solo estilamos un `stVerticalBlock`
              que está anidado dentro de otro (el bloque interno del `st.container`).
            */
            section[data-testid="stMain"] div[data-testid="stVerticalBlock"] div[data-testid="stVerticalBlock"]:has(span.ct-pipeline-ack-float-anchor) {
              position: fixed !important;
              top: max(8vh, 3.5rem) !important;
              left: 50% !important;
              transform: translateX(-50%) !important;
              z-index: 100002 !important;
              width: min(460px, 94vw) !important;
              max-width: 94vw !important;
              margin: 0 !important;
              padding: 1.15rem 1.35rem 1.25rem !important;
              background: rgba(18, 18, 18, 0.97) !important;
              border: 1px solid rgba(158, 220, 104, 0.38) !important;
              border-radius: 14px !important;
              box-shadow: 0 18px 50px rgba(0, 0, 0, 0.55) !important;
            }
            </style>
            <span class="ct-pipeline-ack-float-anchor" aria-hidden="true"></span>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("#### Solicitud registrada")
        st.write(
            "Tu solicitud ha sido registrada. El timeline se actualizará cuando "
            "Airflow comience a correr el pipeline."
        )
        if st.button("Entendido", key="close_pipeline_request_ack_dialog", use_container_width=True):
            st.session_state.pop("show_pipeline_request_ack_dialog", None)
            st.rerun()

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
        /* Misma fila que el botón: título pegado a la izquierda de su columna */
        section[data-testid="stMain"]
          div[data-testid="stHorizontalBlock"]:first-of-type
          > div[data-testid="column"]:nth-child(1)
          [data-testid="stHeadingContainer"] h1,
        section[data-testid="stMain"]
          div[data-testid="stHorizontalBlock"]:first-of-type
          > div[data-testid="column"]:nth-child(1) h1 {
          text-align: left !important;
          margin-left: 0 !important;
          padding-left: 0 !important;
          margin-bottom: 0 !important;
        }
        /* Único botón de esta página: fondo negro fijo, texto lima; hover solo cambia color del texto */
        section[data-testid="stMain"] button[kind="tertiary"] {
            min-height: 0 !important;
            padding-top: 0.34rem !important;
            padding-bottom: 0.34rem !important;
            padding-left: 0.48rem !important;
            padding-right: 0.48rem !important;
            background: #0a0a0a !important;
            background-color: #0a0a0a !important;
            border: 1px solid rgba(222, 255, 154, 0.32) !important;
            box-shadow: none !important;
            color: #deff9a !important;
        }
        section[data-testid="stMain"] div.stButton {
            width: 100% !important;
            display: flex !important;
            justify-content: flex-end !important;
        }
        section[data-testid="stMain"] div.stButton > button[kind="tertiary"] {
            margin-left: auto !important;
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
    title_col, btn_col = st.columns([4.45, 1.25])
    with title_col:
        st.title("Pipeline Live Tracker")
    with btn_col:
        if st.button(
            "▶️ Ejecutar Pipeline",
            key="trigger_pipeline_dag",
            type="tertiary",
            use_container_width=False,
        ):
            try:
                run_id = st.session_state.pipeline_stream.client.trigger_dag_run()
                st.session_state["pipeline_celebrate_run_id"] = run_id
                st.session_state.pop("pipeline_completion_ts", None)
                st.session_state["pipeline_force_inactive_view"] = False
                st.session_state["show_pipeline_request_ack_dialog"] = True
            except RuntimeError as exc:
                st.error(str(exc))
else:
    st.title("Pipeline Live Tracker")

if st.session_state.get("show_pipeline_request_ack_dialog"):
    _render_pipeline_request_ack_float()

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
    completion_ts = st.session_state.get("pipeline_completion_ts")
    force_inactive_view = bool(st.session_state.get("pipeline_force_inactive_view", False))

    if tracks_manual and snapshot.run_state in {"running", "retry"}:
        # Si hay una nueva ejecución activa, salimos del modo inactivo forzado.
        st.session_state["pipeline_force_inactive_view"] = False
        force_inactive_view = False

    if tracks_manual and snapshot.run_state == "done" and not force_inactive_view:
        if completion_ts is None:
            completion_ts = time.time()
            st.session_state["pipeline_completion_ts"] = completion_ts
        if (time.time() - completion_ts) >= COMPLETION_RESET_DELAY_SECONDS:
            st.session_state["pipeline_force_inactive_view"] = True
            st.session_state.pop("pipeline_completion_ts", None)
            st.session_state.pop("pipeline_celebrate_run_id", None)
            force_inactive_view = True
    else:
        st.session_state.pop("pipeline_completion_ts", None)

    display_snapshot = snapshot
    display_celebrate_rid = celebrate_rid
    ui_locked = False

    if force_inactive_view:
        # Reinicio visual a estado inicial tras 20s de mostrar "pipeline finalizado".
        display_snapshot = _idle_pipeline_snapshot(
            snapshot, "Esperando la próxima ejecución del pipeline."
        )
        display_celebrate_rid = None
        ui_locked = True
    elif celebrate_rid is None:
        # Primera vez / sin disparo en esta sesión: no reflejar un run histórico de Airflow como "Completada".
        display_snapshot = _idle_pipeline_snapshot(snapshot, "Esperando ejecución del pipeline.")
        display_celebrate_rid = None
        ui_locked = True
    elif not pipeline_tracks_manual_run(snapshot, celebrate_rid):
        # Ya hubo clic en Ejecutar pero el último snapshot aún es otro run (p. ej. anterior en verde).
        display_snapshot = _starting_pipeline_snapshot(snapshot)
        ui_locked = False

    show_completion_hero = (
        snapshot.run_state == "done"
        and snapshot.run_id is not None
        and celebrate_rid is not None
        and celebrate_rid == snapshot.run_id
        and not force_inactive_view
    )
    sync_data_lake_catalog_from_snapshot(snapshot, celebrate_rid)

    st.session_state.setdefault("bucket_catalog", {"bronze": [], "silver": [], "gold": []})
    st.session_state.setdefault(
        "bucket_list_errors", {"bronze": None, "silver": None, "gold": None}
    )

    render_pipeline_live_stack(
        display_snapshot,
        show_completion_hero=show_completion_hero,
        tracked_manual_run_id=display_celebrate_rid,
        ui_locked=ui_locked,
        bucket_catalog=st.session_state["bucket_catalog"],
        bucket_errors=st.session_state["bucket_list_errors"],
    )

    tracks_display_run = pipeline_tracks_manual_run(display_snapshot, display_celebrate_rid)
    progress_pct = int(display_snapshot.overall_progress * 100) if tracks_display_run else 0
    time_label, time_value = pipeline_timer_metric(display_snapshot, display_celebrate_rid)
    live_mode = "Push" if display_snapshot.source_mode == "push" else "Fallback polling"
    last_upd = display_snapshot.last_updated_at[:19].replace("T", " ")

    st.markdown(
        """
        <style>
        /* Centrar bloques st.html dentro de las columnas de métricas */
        section[data-testid="stMain"] div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
            align-items: center !important;
        }
        section[data-testid="stMain"] div[data-testid="column"] div[data-testid="stElementContainer"] {
            width: 100%;
            display: flex;
            justify-content: center;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    mc_last, mc_mode, mc_time, mc_prog = st.columns(4)
    with mc_last:
        st.html(_pipeline_metric_cell_html("Última actualización", last_upd))
    with mc_mode:
        st.html(_pipeline_metric_cell_html("Modo de actualización", live_mode))
    with mc_time:
        st.html(_pipeline_metric_cell_html(time_label, time_value))
    with mc_prog:
        st.html(_pipeline_metric_cell_html("Progreso global", f"{progress_pct}%"))

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
