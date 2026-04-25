"""
Componente visual del pipeline live.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone

import streamlit as st

from utils.state_schema import PipelineSnapshot


STATE_TEXT = {
    "waiting": "En espera",
    "running": "En ejecución",
    "done": "Completada",
    "error": "Con incidencia",
    "retry": "Reintentando",
}

# Medallas por arquitectura medallón: 5 bronce → 3 plata → 2 oro (orden del pipeline).
_MEDAL_BRONZE_UNTIL = 5
_MEDAL_SILVER_UNTIL = 8


def _medal_tier(index: int) -> tuple[str, str, str]:
    """Devuelve (clase_css, emoji, etiqueta accesible)."""
    if index < _MEDAL_BRONZE_UNTIL:
        return ("medal-bronze", "🥉", "Capa bronce")
    if index < _MEDAL_SILVER_UNTIL:
        return ("medal-silver", "🥈", "Capa plata")
    return ("medal-gold", "🥇", "Capa oro")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_hms(total_seconds: float) -> str:
    secs = int(max(0, total_seconds))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _run_timer_seconds(snapshot: PipelineSnapshot) -> tuple[float | None, str]:
    """
    Returns (seconds, kind) with kind in {'total', 'elapsed', 'none'}.
    """
    start = _parse_iso(snapshot.run_started_at)
    if start is None:
        stage_starts = [
            d for d in (_parse_iso(s.started_at) for s in snapshot.stages) if d is not None
        ]
        if stage_starts:
            start = min(stage_starts)

    end = _parse_iso(snapshot.run_ended_at)
    if snapshot.run_state == "done":
        if end is None:
            stage_ends = [
                d for d in (_parse_iso(s.ended_at) for s in snapshot.stages) if d is not None
            ]
            if stage_ends:
                end = max(stage_ends)
        if start and end:
            return (max(0.0, (end - start).total_seconds()), "total")
        return (None, "none")

    if snapshot.run_state in {"running", "retry"} and start:
        now = datetime.now(timezone.utc)
        return (max(0.0, (now - start).total_seconds()), "elapsed")

    if snapshot.run_state == "error":
        if end is None:
            stage_ends = [
                d for d in (_parse_iso(s.ended_at) for s in snapshot.stages) if d is not None
            ]
            if stage_ends:
                end = max(stage_ends)
        if start and end:
            return (max(0.0, (end - start).total_seconds()), "total")

    return (None, "none")


def pipeline_tracks_manual_run(
    snapshot: PipelineSnapshot, tracked_manual_run_id: str | None
) -> bool:
    """True si el snapshot corresponde al run disparado desde esta sesión (botón Ejecutar)."""
    return (
        tracked_manual_run_id is not None
        and snapshot.run_id is not None
        and tracked_manual_run_id == snapshot.run_id
    )


def pipeline_timer_metric(
    snapshot: PipelineSnapshot, tracked_manual_run_id: str | None = None
) -> tuple[str, str]:
    """Etiqueta y valor para `st.metric` del tiempo (misma lógica que el antiguo temporizador HTML)."""
    if not pipeline_tracks_manual_run(snapshot, tracked_manual_run_id):
        return ("Tiempo de ejecución", "0s")
    secs, timer_kind = _run_timer_seconds(snapshot)
    if secs is None:
        return ("Tiempo de ejecución", "0s")
    tf = _format_hms(secs)
    if timer_kind == "total":
        return ("Tiempo de ejecución", tf)
    return ("Transcurrido", tf)


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


def render_pipeline_timeline(
    snapshot: PipelineSnapshot,
    *,
    show_completion_hero: bool = False,
    tracked_manual_run_id: str | None = None,
) -> None:
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
            border: 1px solid #2d2d2d; background: #181818; border-radius: 12px; padding: 0.65rem 0.55rem 0.6rem;
            min-height: 72px;
          }
          .stage-head {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.45rem;
            text-align: center;
          }
          .stage-medal {
            width: 2.15rem;
            height: 2.15rem;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.12rem;
            line-height: 1;
            border: 1px solid rgba(255,255,255,0.12);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.15), 0 2px 6px rgba(0,0,0,0.35);
          }
          .medal-bronze {
            background: radial-gradient(circle at 32% 28%, #e8a060, #8b4513 58%, #4a2509 100%);
          }
          .medal-silver {
            background: radial-gradient(circle at 32% 28%, #f2f2f2, #a8aeb8 55%, #5c636b 100%);
          }
          .medal-gold {
            background: radial-gradient(circle at 32% 28%, #fff4b8, #e8c547 50%, #a67c00 100%);
          }
          .stage-body {
            width: 100%;
            text-align: center;
          }
          .stage-card .title {
            margin: 0;
            color: #ececec;
            font-size: 0.84rem;
            font-weight: 600;
            line-height: 1.25;
            text-align: center;
          }
          .stage-card .meta {
            margin-top: 0.24rem;
            font-size: 0.76rem;
            color: #b8b8b8;
            text-align: center;
          }
          .stage-card.running { border-color: rgba(94,175,255,0.6); }
          .stage-card.done { border-color: rgba(91,196,112,0.62); }
          .stage-card.error { border-color: rgba(245,96,96,0.62); }
          .stage-card.retry { border-color: rgba(255,180,70,0.62); }
          .completion-hero {
            margin: 0.85rem 0 0.35rem 0;
            padding: 0.85rem 1rem;
            border-radius: 14px;
            text-align: center;
            font-size: clamp(1.35rem, 2.8vw, 1.85rem);
            font-weight: 800;
            line-height: 1.2;
            color: #0d1408;
            background: linear-gradient(120deg, #c8f090 0%, #deff9a 45%, #b8e86a 100%);
            border: 1px solid rgba(222,255,154,0.85);
            box-shadow: 0 0 28px rgba(222,255,154,0.28);
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    tracks = pipeline_tracks_manual_run(snapshot, tracked_manual_run_id)
    progress_pct = int(snapshot.overall_progress * 100) if tracks else 0
    object_left = min(max(progress_pct, 2), 100)

    completion_block = ""
    if show_completion_hero:
        completion_block = (
            '<div class="completion-hero" role="status">¡Están listos tus datos!</div>'
        )

    pipeline_header = (
        f'<div class="pipeline-shell">'
        f'<div class="pipeline-top"><h3>{html.escape(snapshot.dag_id)}</h3>{_status_chip(snapshot)}</div>'
        f"{completion_block}"
        f'<div class="line-wrap"><div class="line-track">'
        f'<div class="line-progress" style="width:{progress_pct}%;"></div>'
        f'<div class="line-object" style="left:{object_left}%;"></div>'
        f"</div></div>"
    )

    stage_cards = []
    for index, stage in enumerate(snapshot.stages):
        medal_class, medal_emoji, medal_label = _medal_tier(index)
        medal_escaped = html.escape(medal_emoji)
        medal_aria = html.escape(medal_label)
        label = html.escape(stage.label)
        state_text = STATE_TEXT.get(stage.state, "En espera")
        stage_cards.append(
            (
                f'<div class="stage-card {html.escape(stage.state)}">'
                f'<div class="stage-head">'
                f'<span class="stage-medal {medal_class}" title="{medal_aria}" role="img" aria-label="{medal_aria}">'
                f"{medal_escaped}</span>"
                f'<div class="stage-body">'
                f'<p class="title">{label}</p>'
                f'<div class="meta">{state_text}</div>'
                f"</div></div></div>"
            )
        )

    st.markdown(
        pipeline_header + f'<div class="stage-grid">{"".join(stage_cards)}</div></div>',
        unsafe_allow_html=True,
    )
