"""
Componente visual del pipeline live.
"""

from __future__ import annotations

import html

import streamlit as st

from utils.state_schema import PipelineSnapshot


STATE_TEXT = {
    "waiting": "En espera",
    "running": "En ejecución",
    "done": "Completada",
    "error": "Con incidencia",
    "retry": "Reintentando",
}


def _status_chip(snapshot: PipelineSnapshot) -> str:
    mapping = {
        "waiting": ("status-waiting", "Esperando ejecución"),
        "running": ("status-running", "Pipeline en movimiento"),
        "done": ("status-done", "Pipeline finalizado"),
        "error": ("status-error", "Pipeline con incidencia"),
        "retry": ("status-retry", "Pipeline reintentando"),
    }
    css, label = mapping.get(snapshot.run_state, ("status-waiting", "Estado desconocido"))
    return f'<span class="pipeline-status {css}">{label}</span>'


def render_pipeline_timeline(snapshot: PipelineSnapshot) -> None:
    st.markdown(
        """
        <style>
          .pipeline-shell {
            background: linear-gradient(160deg, rgba(18,18,18,1) 0%, rgba(12,12,12,1) 100%);
            border: 1px solid rgba(222,255,154,0.22);
            border-radius: 16px;
            padding: 1.1rem 1.1rem 1.3rem;
            margin: 0.4rem 0 0.8rem 0;
          }
          .pipeline-top { display: flex; align-items: center; justify-content: space-between; gap: 0.75rem; }
          .pipeline-top h3 { margin: 0; color: #f6ffe6; font-size: 1.08rem; font-weight: 700; }
          .pipeline-sub { color: #bfbfbf; font-size: 0.86rem; margin-top: 0.3rem; }
          .pipeline-status {
            font-size: 0.78rem; font-weight: 700; border-radius: 999px; padding: 0.27rem 0.65rem;
            border: 1px solid transparent; letter-spacing: 0.03em;
          }
          .status-waiting { background: rgba(145,145,145,0.16); color: #d9d9d9; border-color: rgba(145,145,145,0.4); }
          .status-running { background: rgba(70,150,255,0.15); color: #8fc4ff; border-color: rgba(70,150,255,0.5); }
          .status-done { background: rgba(91,196,112,0.14); color: #85e09a; border-color: rgba(91,196,112,0.46); }
          .status-error { background: rgba(245,96,96,0.14); color: #ff8f8f; border-color: rgba(245,96,96,0.46); }
          .status-retry { background: rgba(255,180,70,0.15); color: #ffc579; border-color: rgba(255,180,70,0.52); }
          .line-wrap { position: relative; margin: 1.2rem 0 0.5rem; }
          .line-track {
            position: relative; width: 100%; height: 8px; border-radius: 999px;
            background: linear-gradient(90deg, #2a2a2a 0%, #363636 100%);
            border: 1px solid #3d3d3d;
          }
          .line-progress {
            position: absolute; top: 0; left: 0; height: 100%; border-radius: 999px;
            background: linear-gradient(90deg, #9edc68 0%, #deff9a 100%);
            box-shadow: 0 0 18px rgba(222,255,154,0.35);
            transition: width 900ms ease;
          }
          .line-object {
            position: absolute; top: 50%; transform: translate(-50%, -50%);
            width: 18px; height: 18px; border-radius: 50%;
            background: radial-gradient(circle at 30% 30%, #fff8d8, #deff9a 55%, #8abf50 100%);
            border: 2px solid rgba(9,9,9,0.5);
            box-shadow: 0 0 18px rgba(222,255,154,0.58);
            transition: left 900ms ease;
            animation: pulseGlow 1600ms infinite;
          }
          @keyframes pulseGlow {
            0% { box-shadow: 0 0 10px rgba(222,255,154,0.4); }
            50% { box-shadow: 0 0 22px rgba(222,255,154,0.8); }
            100% { box-shadow: 0 0 10px rgba(222,255,154,0.4); }
          }
          .stage-grid {
            margin-top: 0.9rem;
            display: grid; gap: 0.45rem;
            grid-template-columns: repeat(auto-fit, minmax(175px, 1fr));
          }
          .stage-card {
            border: 1px solid #2d2d2d; background: #181818; border-radius: 12px; padding: 0.55rem 0.65rem;
            min-height: 62px;
          }
          .stage-card .title { margin: 0; color: #ececec; font-size: 0.84rem; font-weight: 600; line-height: 1.25; }
          .stage-card .meta { margin-top: 0.24rem; font-size: 0.76rem; color: #b8b8b8; }
          .stage-card.running { border-color: rgba(94,175,255,0.6); }
          .stage-card.done { border-color: rgba(91,196,112,0.62); }
          .stage-card.error { border-color: rgba(245,96,96,0.62); }
          .stage-card.retry { border-color: rgba(255,180,70,0.62); }
        </style>
        """,
        unsafe_allow_html=True,
    )

    progress_pct = int(snapshot.overall_progress * 100)
    object_left = min(max(progress_pct, 2), 100)
    run_label = html.escape(snapshot.run_id or "sin run activa")
    message = html.escape(snapshot.message or "Monitoreando pipeline en tiempo real.")
    pipeline_header = (
        f'<div class="pipeline-shell">'
        f'<div class="pipeline-top"><h3>{html.escape(snapshot.dag_id)}</h3>{_status_chip(snapshot)}</div>'
        f'<div class="pipeline-sub">Run: {run_label} · {message}</div>'
        f'<div class="line-wrap"><div class="line-track">'
        f'<div class="line-progress" style="width:{progress_pct}%;"></div>'
        f'<div class="line-object" style="left:{object_left}%;"></div>'
        f"</div></div>"
    )

    stage_cards = []
    for stage in snapshot.stages:
        label = html.escape(stage.label)
        state_text = STATE_TEXT.get(stage.state, "En espera")
        stage_cards.append(
            (
                f'<div class="stage-card {html.escape(stage.state)}">'
                f'<p class="title">{label}</p>'
                f'<div class="meta">{state_text}</div>'
                f"</div>"
            )
        )

    st.markdown(
        pipeline_header + f'<div class="stage-grid">{"".join(stage_cards)}</div></div>',
        unsafe_allow_html=True,
    )
