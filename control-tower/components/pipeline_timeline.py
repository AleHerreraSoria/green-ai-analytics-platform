"""
Componente visual del pipeline live.
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from typing import Any

import streamlit.components.v1 as components

from utils.state_schema import PipelineSnapshot


STATE_TEXT = {
    "waiting":  "En espera",
    "running":  "En ejecución",
    "done":     "Completada",
    "error":    "Con incidencia",
    "retry":    "Reintentando",
}

_MEDAL_BRONZE_UNTIL = 5
_MEDAL_SILVER_UNTIL = 8

# =============================================================================
# Estructura visual avanzada del modal y mapeo de negocio 
# rediseñado y añadido por el ingeniero Jose David Frias
# =============================================================================
STAGE_BUSINESS_DESCRIPTIONS: dict[str, dict[str, Any]] = {
    "Ingestando Datos Historicos": {
        "desc": "Carga de archivos locales hacia el data lake usando boto3.",
        "metrics": ["Destino: Bucket S3 'sources'", "Exclusión dinámica de temporales"],
        "highlight": "Capa Bronze: Ingesta Raw"
    },
    "Generando Dataset de Precios": {
        "desc": "Integración de tarifas eléctricas y costos de mercado.",
        "metrics": ["Validación de prefijos en S3", "Verificación de peso (> 0 bytes)"],
        "highlight": "Capa Bronze: Quality Check"
    },
    "Generando Logs Sintéticos": {
        "desc": "Simulación de escenarios de uso de servidores AI.",
        "metrics": ["Ingesta estructurada", "Aislamiento de entorno"],
        "highlight": "Capa Bronze: Raw Data"
    },
    "Ingestando Datos de API": {
        "desc": "Conexión a sensores IoT para métricas de GPU y huella de carbono.",
        "metrics": ["Descarga de muestra JSON", "Validación parseable (json.loads)"],
        "highlight": "Capa Bronze: Streaming API"
    },
    "Validando capa Bronze": {
        "desc": "Auditoría estructural antes del procesamiento distribuido en Spark.",
        "metrics": ["Verificación de archivos críticos (geo_cloud)", "Garantía de integridad inicial"],
        "highlight": "Capa Bronze: Quality Gate"
    },
    "Transformando a Silver": {
        "desc": "Limpieza, estandarización y modelado lógico de datos crudos.",
        "metrics": ["Estandarización a snake_case", "Aplanado de JSONs (explode)", "Self-Healing: descarte de nulos (ansi.enabled=false)"],
        "highlight": "Capa Silver: Cleansed & Conformed"
    },
    "Validando capa Silver": {
        "desc": "Filtro de dominios de negocio inválidos y limpieza en memoria.",
        "metrics": ["Filtro de intensidad de carbono < 0", "Validación de rangos matemáticos GPU", "Limpieza con pandas (skiprows)"],
        "highlight": "Capa Silver: Business Rules"
    },
    "Auditando calidad Silver": {
        "desc": "Escritura optimizada de datos limpios y enriquecidos.",
        "metrics": ["Cálculo derivado: precio_red_estimado_usd", "Escritura en formato Delta Lake", "Sobrescritura dinámica de particiones"],
        "highlight": "Capa Silver: Data Optimization"
    },
    "Construyendo capa Gold": {
        "desc": "Construcción del modelo Kimball (Esquema Estrella) para BI.",
        "metrics": ["Hechos: fact_ai_compute_usage", "Dimensiones: dim_gpu_model, dim_country", "Generación Surrogate Keys (Hash MD5)"],
        "highlight": "Capa Gold: Dimensional Modeling"
    },
    "Validando capa Gold": {
        "desc": "Cálculo de métricas finales y agregaciones de Green AI.",
        "metrics": ["KPI: cost_electricity_usd", "KPI: cost_compute_usd", "Agregación: fact_carbon_intensity_hourly"],
        "highlight": "Capa Gold: Analytics Ready"
    },
}

# Escala visual del modal respecto al diseño base (1.0 = 100%). 1.5 = 150 % en todos los elementos.
_MODAL_VISUAL_SCALE = 1.5

# Estilos del modal montado en document.parent (página principal), prefijo .ptm- para no chocar con Streamlit.
_MODAL_PORTAL_CSS = (
    """
#pipeline-timeline-modal-root.ptm-overlay {
  display: none;
  position: fixed;
  inset: 0;
  z-index: 2147483000;
  background: rgba(0,0,0,0.45);
  backdrop-filter: blur(5px);
  -webkit-backdrop-filter: blur(5px);
  overflow-y: auto;
  overscroll-behavior: contain;
  -webkit-overflow-scrolling: touch;
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif;
}
#pipeline-timeline-modal-root.ptm-overlay.ptm-open { display: block; }
#pipeline-timeline-modal-root .ptm-inner {
  min-height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.25rem 1rem 2rem;
  box-sizing: border-box;
}
#pipeline-timeline-modal-root .ptm-card {
  background: linear-gradient(150deg, #1e1e1e 0%, #141414 100%);
  border: 1px solid rgba(222,255,154,.26);
  border-radius: 18px;
  padding: 1.5rem 1.75rem 1.4rem;
  width: min(420px, 92vw);
  position: relative;
  box-shadow:
    0 0 0 1px rgba(222,255,154,.05),
    0 20px 55px rgba(0,0,0,.62),
    0 4px 14px rgba(0,0,0,.38);
  animation: ptmModalIn .22s cubic-bezier(.16,1,.3,1) both;
  zoom: """
    + str(_MODAL_VISUAL_SCALE)
    + """;
}
@keyframes ptmModalIn {
  from { opacity: 0; transform: scale(.91) translateY(14px); }
  to   { opacity: 1; transform: scale(1)   translateY(0); }
}
#pipeline-timeline-modal-root .ptm-close {
  position: absolute; top: 14px; right: 14px;
  width: 28px; height: 28px; border-radius: 50%;
  background: rgba(255,255,255,.05);
  border: 1px solid rgba(255,255,255,.09);
  color: #888; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: all .14s;
}
#pipeline-timeline-modal-root .ptm-close:hover {
  background: rgba(222,255,154,.15); border-color: rgba(222,255,154,.4); color: #deff9a;
}
#pipeline-timeline-modal-root .ptm-header-flex {
  display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;
}
#pipeline-timeline-modal-root .ptm-medal {
  width: 3.5rem; height: 3.5rem; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 1.8rem; flex-shrink: 0;
  border: 1px solid rgba(255,255,255,.1);
  box-shadow: 0 4px 14px rgba(0,0,0,.42);
}
#pipeline-timeline-modal-root .ptm-medal.medal-bronze {
  background: radial-gradient(circle at 32% 28%, #e8a060, #8b4513 58%, #4a2509 100%);
}
#pipeline-timeline-modal-root .ptm-medal.medal-silver {
  background: radial-gradient(circle at 32% 28%, #f2f2f2, #a8aeb8 55%, #5c636b 100%);
}
#pipeline-timeline-modal-root .ptm-medal.medal-gold {
  background: radial-gradient(circle at 32% 28%, #fff4b8, #e8c547 50%, #a67c00 100%);
}
#pipeline-timeline-modal-root .ptm-header-text {
  display: flex; flex-direction: column; justify-content: center;
}
#pipeline-timeline-modal-root .ptm-eyebrow {
  font-size: .68rem; font-weight: 700; letter-spacing: .08em;
  text-transform: uppercase; color: rgba(222,255,154,.6);
  margin-bottom: .2rem;
}
#pipeline-timeline-modal-root .ptm-title {
  font-size: 1.15rem; font-weight: 700; color: #f0ffe0;
  line-height: 1.2;
}
#pipeline-timeline-modal-root .ptm-divider {
  border: none; height: 1px;
  background: linear-gradient(90deg, rgba(222,255,154,.3), transparent);
  margin-bottom: 1rem;
}
#pipeline-timeline-modal-root .ptm-highlight {
  display: inline-block;
  background: rgba(222,255,154,.1);
  color: #deff9a;
  padding: 0.35rem 0.75rem;
  border-radius: 6px;
  font-size: 0.8rem;
  font-weight: 600;
  margin-bottom: 0.85rem;
  border: 1px solid rgba(222,255,154,.2);
}
#pipeline-timeline-modal-root .ptm-desc {
  font-size: .9rem; color: #c4c4c4; line-height: 1.5;
  margin-bottom: 1rem;
}
#pipeline-timeline-modal-root .ptm-metrics {
  list-style: none; padding: 0; margin: 0 0 1.2rem 0;
  display: flex; flex-direction: column; gap: 0.45rem;
}
#pipeline-timeline-modal-root .ptm-metrics li {
  font-size: 0.82rem; color: #e0e0e0;
  display: flex; align-items: flex-start; gap: 0.45rem;
  background: rgba(255,255,255,.03);
  padding: 0.4rem 0.6rem;
  border-radius: 6px;
  border: 1px solid rgba(255,255,255,.05);
}
#pipeline-timeline-modal-root .ptm-metrics .check-icon {
  color: #8abf50; font-weight: bold; font-size: 0.9rem; line-height: 1;
}
#pipeline-timeline-modal-root .ptm-badge {
  display: inline-flex; align-items: center; justify-content: center; gap: .35rem;
  background: rgba(255,255,255,.05);
  border: 1px solid rgba(255,255,255,.15);
  border-radius: 999px; padding: .35rem 1rem;
  font-size: .78rem; font-weight: 600; color: #fff;
}
"""
)

# Marcado del modal en la ventana principal (inyectado vía JS).
_PORTAL_SHELL_HTML = (
    '<div class="ptm-inner">'
    '<div class="ptm-card">'
    '<button type="button" class="ptm-close" id="ptm-close-btn" aria-label="Cerrar"'
    ' onclick="try{window.parent.__pipelineTimelineCloseModal&&window.parent.__pipelineTimelineCloseModal()}catch(e){}">'
    '<svg width="12" height="12" viewBox="0 0 12 12" fill="none">'
    '<path d="M1 1l10 10M11 1L1 11" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>'
    '</svg>'
    '</button>'
    '<div class="ptm-header-flex">'
    '<div id="ptm-medal" class="ptm-medal medal-bronze"></div>'
    '<div class="ptm-header-text">'
    '<p class="ptm-eyebrow">Análisis de Pipeline</p>'
    '<p id="ptm-title" class="ptm-title"></p>'
    '</div></div>'
    '<hr class="ptm-divider">'
    '<div id="ptm-highlight" class="ptm-highlight"></div>'
    '<p id="ptm-desc" class="ptm-desc"></p>'
    '<ul id="ptm-metrics" class="ptm-metrics"></ul>'
    '<div id="ptm-badge" class="ptm-badge"></div>'
    '</div></div>'
)


def _medal_tier(index: int) -> tuple[str, str, str]:
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
    start = _parse_iso(snapshot.run_started_at)
    if start is None:
        stage_starts = [d for d in (_parse_iso(s.started_at) for s in snapshot.stages) if d]
        if stage_starts:
            start = min(stage_starts)

    end = _parse_iso(snapshot.run_ended_at)
    if snapshot.run_state == "done":
        if end is None:
            stage_ends = [d for d in (_parse_iso(s.ended_at) for s in snapshot.stages) if d]
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
            stage_ends = [d for d in (_parse_iso(s.ended_at) for s in snapshot.stages) if d]
            if stage_ends:
                end = max(stage_ends)
        if start and end:
            return (max(0.0, (end - start).total_seconds()), "total")

    return (None, "none")


def pipeline_tracks_manual_run(
    snapshot: PipelineSnapshot, tracked_manual_run_id: str | None
) -> bool:
    return (
        tracked_manual_run_id is not None
        and snapshot.run_id is not None
        and tracked_manual_run_id == snapshot.run_id
    )


def pipeline_timer_metric(
    snapshot: PipelineSnapshot, tracked_manual_run_id: str | None = None
) -> tuple[str, str]:
    if not pipeline_tracks_manual_run(snapshot, tracked_manual_run_id):
        return ("Tiempo de ejecución", "0s")
    secs, timer_kind = _run_timer_seconds(snapshot)
    if secs is None:
        return ("Tiempo de ejecución", "0s")
    tf = _format_hms(secs)
    return ("Tiempo de ejecución", tf) if timer_kind == "total" else ("Transcurrido", tf)


def _status_chip_html(snapshot: PipelineSnapshot) -> str:
    mapping = {
        "waiting": ("status-waiting", "Esperando ejecución"),
        "running": ("status-running", "Pipeline en movimiento"),
        "done":    ("status-done",    "Pipeline finalizado"),
        "error":   ("status-error",   "Pipeline con incidencia"),
        "retry":   ("status-retry",   "Pipeline reintentando"),
    }
    css, label = mapping.get(snapshot.run_state, ("status-waiting", "Estado desconocido"))
    return f'<span class="pipeline-status {css}">{label}</span>'


def _build_pipeline_shell_fragment(
    snapshot: PipelineSnapshot,
    progress_pct: int,
    object_left: int,
    show_completion_hero: bool,
    *,
    ui_locked: bool = False,
) -> tuple[str, str, str]:
    """
    Fragmento visual del timeline (solo div.pipeline-shell) + JSONs para el script.
    Devuelve (html_shell, desc_json, stage_states_json).
    """
    cards_parts: list[str] = []
    for i, stage in enumerate(snapshot.stages):
        medal_class, medal_emoji, medal_label = _medal_tier(i)
        if ui_locked:
            state_text = "Bloqueado"
            card_css_state = "waiting"
        else:
            state_text = STATE_TEXT.get(stage.state, "En espera")
            card_css_state = stage.state

        d_title = html.escape(stage.label, quote=True)
        d_mclass = html.escape(medal_class, quote=True)
        d_memoji = html.escape(medal_emoji, quote=True)
        d_state = html.escape(state_text, quote=True)

        label_e = html.escape(stage.label)
        emoji_e = html.escape(medal_emoji)
        aria_e = html.escape(medal_label)
        card_e = html.escape(card_css_state)
        lock_cls = " locked" if ui_locked else ""
        if ui_locked:
            meta_line = (
                '<p class="s-meta s-meta-with-lock">'
                '<span class="s-lock" title="Ejecutá el pipeline para ver el avance real" aria-hidden="true">🔒</span>'
                "<span>Bloqueado</span>"
                "</p>"
            )
        else:
            meta_line = f'<p class="s-meta">{html.escape(state_text)}</p>'

        cards_parts.append(
            f'<div class="stage-card {card_e}{lock_cls}">'
            f'<button class="info-btn js-info"'
            f' data-title="{d_title}"'
            f' data-mclass="{d_mclass}"'
            f' data-memoji="{d_memoji}"'
            f' data-state="{d_state}"'
            f' aria-label="Más información: {label_e}"'
            f' title="¿Qué es esta etapa?">i</button>'
            f'<div class="stage-head">'
            f'<span class="stage-medal {medal_class}" role="img" aria-label="{aria_e}">'
            f"{emoji_e}</span>"
            f'<div class="stage-body">'
            f'<p class="s-title">{label_e}</p>'
            f"{meta_line}"
            f"</div></div>"
            f"</div>"
        )

    hero_block = (
        '<div class="completion-hero" role="status">¡Están listos tus datos!</div>'
        if show_completion_hero
        else ""
    )

    status_chip = _status_chip_html(snapshot)
    dag_id_e = html.escape(snapshot.dag_id)

    desc_json = json.dumps(STAGE_BUSINESS_DESCRIPTIONS, ensure_ascii=False)

    if ui_locked:
        stage_state_map = {s.label: "Bloqueado" for s in snapshot.stages}
    else:
        stage_state_map = {
            s.label: STATE_TEXT.get(s.state, "En espera") for s in snapshot.stages
        }
    stage_states_json = json.dumps(stage_state_map, ensure_ascii=False)

    cards_html = "".join(cards_parts)

    shell = (
        f'<div class="pipeline-shell">'
        f'  <div class="pipeline-top">'
        f"    <h3>{dag_id_e}</h3>"
        f"    {status_chip}"
        f"  </div>"
        f"  {hero_block}"
        f'  <div class="line-wrap">'
        f'    <div class="line-track">'
        f'      <div class="line-progress" style="width:{progress_pct}%"></div>'
        f'      <div class="line-object"   style="left:{object_left}%"></div>'
        f"    </div>"
        f"  </div>"
        f'  <div class="stage-grid">{cards_html}</div>'
        f"</div>"
    )
    return shell, desc_json, stage_states_json


def build_timeline_stack_html_and_script(
    snapshot: PipelineSnapshot,
    progress_pct: int,
    object_left: int,
    show_completion_hero: bool,
    *,
    ui_locked: bool = False,
) -> tuple[str, str]:
    """(fragmento pipeline-shell, etiqueta script completa) para empilar con otros bloques."""
    shell, desc_json, stage_states_json = _build_pipeline_shell_fragment(
        snapshot,
        progress_pct,
        object_left,
        show_completion_hero,
        ui_locked=ui_locked,
    )
    script = "<script>" + _get_js(desc_json, _MODAL_PORTAL_CSS, stage_states_json) + "</script>"
    return shell, script


def _build_html(
    snapshot: PipelineSnapshot,
    progress_pct: int,
    object_left: int,
    show_completion_hero: bool,
    *,
    ui_locked: bool = False,
) -> str:
    """
    Construye el documento HTML completo del componente de pipeline.
    """
    shell, desc_json, stage_states_json = _build_pipeline_shell_fragment(
        snapshot,
        progress_pct,
        object_left,
        show_completion_hero,
        ui_locked=ui_locked,
    )

    return (
        "<!DOCTYPE html>"
        '<html lang="es">'
        "<head>"
        '<meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<style>" + _get_css() + "</style>"
        "</head>"
        "<body>"
        + shell
        + "<script>"
        + _get_js(desc_json, _MODAL_PORTAL_CSS, stage_states_json)
        + "</script>"
        "</body>"
        "</html>"
    )


def _get_css() -> str:
    return """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif;
  background: transparent;
}

/* Pipeline shell */
.pipeline-shell {
  background: linear-gradient(160deg, #121212 0%, #0c0c0c 100%);
  border: 1px solid rgba(222,255,154,.2);
  border-radius: 16px;
  padding: 1rem 1.1rem 1.1rem;
}
.pipeline-top {
  display: flex; align-items: center;
  justify-content: space-between; gap: .75rem;
}
.pipeline-top h3 {
  color: #f0ffe0; font-size: 1rem;
  font-weight: 700; letter-spacing: .01em;
}

/* Status chips */
.pipeline-status {
  font-size: .72rem; font-weight: 700; border-radius: 999px;
  padding: .2rem .6rem; border: 1px solid transparent;
  letter-spacing: .03em; white-space: nowrap;
}
.status-waiting { background: rgba(145,145,145,.14); color: #ccc;    border-color: rgba(145,145,145,.36); }
.status-running { background: rgba(70,150,255,.13);  color: #8fc4ff; border-color: rgba(70,150,255,.45); }
.status-done    { background: rgba(91,196,112,.12);  color: #85e09a; border-color: rgba(91,196,112,.42); }
.status-error   { background: rgba(245,96,96,.12);   color: #ff8f8f; border-color: rgba(245,96,96,.42); }
.status-retry   { background: rgba(255,180,70,.13);  color: #ffc579; border-color: rgba(255,180,70,.48); }

/* Progress bar */
.line-wrap { position: relative; margin: .85rem 0 .4rem; }
.line-track {
  position: relative; width: 100%; height: 7px;
  border-radius: 999px; background: #1e1e1e; border: 1px solid #2e2e2e;
}
.line-progress {
  position: absolute; top: 0; left: 0; height: 100%;
  border-radius: 999px;
  background: linear-gradient(90deg, #9edc68, #deff9a);
  box-shadow: 0 0 10px rgba(222,255,154,.28);
  transition: width .9s ease;
}
.line-object {
  position: absolute; top: 50%; transform: translate(-50%,-50%);
  width: 15px; height: 15px; border-radius: 50%;
  background: radial-gradient(circle at 32% 30%, #fff8d8, #deff9a 55%, #8abf50 100%);
  border: 2px solid rgba(0,0,0,.45);
  box-shadow: 0 0 12px rgba(222,255,154,.5);
  animation: glow 1.6s infinite;
}
@keyframes glow {
  0%,100% { box-shadow: 0 0 7px rgba(222,255,154,.35); }
  50%      { box-shadow: 0 0 18px rgba(222,255,154,.72); }
}

/* Stage grid */
.stage-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
  gap: .38rem;
  margin-top: .5rem;
}

/* Stage card */
.stage-card {
  position: relative; background: #181818;
  border: 1px solid #282828; border-radius: 10px;
  padding: .55rem .48rem .5rem;
  transition: border-color .18s, box-shadow .18s;
}
.stage-card.running { border-color: rgba(94,175,255,.52); }
.stage-card.done    { border-color: rgba(91,196,112,.52); }
.stage-card.error   { border-color: rgba(245,96,96,.52); }
.stage-card.retry   { border-color: rgba(255,180,70,.52); }
.stage-card.locked {
  border-color: rgba(88, 88, 88, 0.55);
  background: #141414;
  opacity: 0.95;
}
.s-meta-with-lock {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.28rem;
  flex-wrap: wrap;
}
.s-meta-with-lock .s-lock {
  font-size: 0.72em;
  line-height: 1;
  user-select: none;
}

.stage-head { display: flex; flex-direction: column; align-items: center; gap: .35rem; text-align: center; }
.stage-medal {
  width: 1.9rem; height: 1.9rem; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: .95rem; line-height: 1;
  border: 1px solid rgba(255,255,255,.1);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.1), 0 2px 5px rgba(0,0,0,.32);
}
.medal-bronze { background: radial-gradient(circle at 32% 28%, #e8a060, #8b4513 58%, #4a2509 100%); }
.medal-silver { background: radial-gradient(circle at 32% 28%, #f2f2f2, #a8aeb8 55%, #5c636b 100%); }
.medal-gold   { background: radial-gradient(circle at 32% 28%, #fff4b8, #e8c547 50%, #a67c00 100%); }

.stage-body { width: 100%; }
.s-title { color: #e0e0e0; font-size: .78rem; font-weight: 600; line-height: 1.22; margin-bottom: .16rem; }
.s-meta  { font-size: .7rem; color: #888; }

/* Info button */
.info-btn {
  position: absolute; top: 4px; right: 5px;
  width: 17px; height: 17px; border-radius: 50%;
  background: rgba(222,255,154,.06);
  border: 1px solid rgba(222,255,154,.18);
  color: rgba(222,255,154,.55);
  font-size: .62rem; font-weight: 700; font-style: italic;
  cursor: pointer; line-height: 1;
  display: flex; align-items: center; justify-content: center;
  transition: background .14s, border-color .14s, color .14s, transform .1s;
  padding: 0;
}
.info-btn:hover {
  background: rgba(222,255,154,.16);
  border-color: rgba(222,255,154,.5);
  color: #deff9a;
  transform: scale(1.12);
}
.info-btn:active { transform: scale(.92); }

/* Completion hero */
.completion-hero {
  margin: .65rem 0 .3rem; padding: .7rem 1rem;
  border-radius: 12px; text-align: center;
  font-size: 1.45rem; font-weight: 800; color: #0d1408;
  background: linear-gradient(120deg, #c8f090, #deff9a 45%, #b8e86a 100%);
  border: 1px solid rgba(222,255,154,.78);
  box-shadow: 0 0 22px rgba(222,255,154,.22);
}
"""


def _get_js(desc_json: str, modal_css: str, stage_states_json: str) -> str:
    """
    Modal en documento padre (página Streamlit). El cierre usa window.parent
    para sobrevivir a los reruns del iframe (Streamlit); el badge se sincroniza
    con STAGE_STATE_MAP en cada recarga del componente.
    """
    css_lit = json.dumps(modal_css)
    shell_lit = json.dumps(_PORTAL_SHELL_HTML)
    return (
        "(function(){"
        "var D=" + desc_json + ";"
        "var STAGE_STATE_MAP=" + stage_states_json + ";"
        "var MODAL_CSS=" + css_lit + ";"
        "var PORTAL_HTML=" + shell_lit + ";"
        "var PID='pipeline-timeline-modal-root';var SID='pipeline-timeline-modal-styles';"
        "function targetDoc(){try{if(window.parent!==window){return window.parent.document}}"
        "catch(e){}return document;}"
        "function ensureStyles(doc){if(doc.getElementById(SID))return;"
        "var st=doc.createElement('style');st.id=SID;st.textContent=MODAL_CSS;doc.head.appendChild(st);}"
        "function removePortal(doc){var r=doc.getElementById(PID);"
        "if(r&&r.parentNode)r.parentNode.removeChild(r);}"
        "function closeModal(){removePortal(targetDoc());}"
        "window.parent.__pipelineTimelineCloseModal=function(){try{closeModal();}catch(e){}};"
        "if(!window.parent.__pipelineTimelineEscInstalled){"
        "window.parent.__pipelineTimelineEscInstalled=1;"
        "window.parent.addEventListener('keydown',function(ev){"
        "if(ev.key!=='Escape')return;"
        "var fn=window.parent.__pipelineTimelineCloseModal;if(fn)fn();});}"
        "function refreshOpenModal(){var doc=targetDoc();var root=doc.getElementById(PID);"
        "if(!root||!root.classList.contains('ptm-open'))return;"
        "var te=root.querySelector('#ptm-title');var be=root.querySelector('#ptm-badge');"
        "if(!te||!be)return;var lbl=te.textContent;var nv=STAGE_STATE_MAP[lbl];"
        "if(typeof nv==='string'){be.textContent='\\u26a1 '+nv;}}"
        "function mountPortal(doc){ensureStyles(doc);removePortal(doc);"
        "var el=doc.createElement('div');el.id=PID;el.className='ptm-overlay';"
        "el.setAttribute('role','dialog');el.setAttribute('aria-modal','true');"
        "el.setAttribute('aria-labelledby','ptm-title');el.innerHTML=PORTAL_HTML;"
        "el.setAttribute('onclick',"
        "'try{var t=event.target;if(t&&t.closest&&!t.closest(\".ptm-card\")){"
        "window.parent.__pipelineTimelineCloseModal&&window.parent.__pipelineTimelineCloseModal();}"
        "}catch(e){}');"
        "doc.body.appendChild(el);return el;}"
        "function openModal(t,mc,me,s){"
        "closeModal();"
        "var doc=targetDoc();"
        "var data=D[t]||{desc:'Procesando etapa del pipeline.',metrics:[],highlight:''};"
        "var root=mountPortal(doc);"
        "var mm=root.querySelector('#ptm-medal');var mt=root.querySelector('#ptm-title');"
        "var md=root.querySelector('#ptm-desc');var mh=root.querySelector('#ptm-highlight');"
        "var ml=root.querySelector('#ptm-metrics');var mb=root.querySelector('#ptm-badge');"
        "mm.className='ptm-medal '+mc;mm.textContent=me;mt.textContent=t;"
        "md.textContent=data.desc;"
        "if(data.highlight){mh.style.display='inline-block';mh.textContent=data.highlight;}"
        "else{mh.style.display='none';}"
        "var metricsHtml='',i;"
        "if(data.metrics&&data.metrics.length){"
        "for(i=0;i<data.metrics.length;i++){"
        "metricsHtml+='<li><span class=\"check-icon\">\u2713</span> '+data.metrics[i]+'</li>';}}"
        "ml.innerHTML=metricsHtml;"
        "mb.textContent='\\u26a1 '+s;"
        "var nv2=STAGE_STATE_MAP[t];if(typeof nv2==='string'){mb.textContent='\\u26a1 '+nv2;}"
        "root.classList.add('ptm-open');"
        "doc.getElementById('ptm-close-btn').focus();"
        "}"
        "document.addEventListener('click',function(e){"
        "var b=e.target.closest('.js-info');if(!b)return;"
        "e.stopPropagation();"
        "openModal(b.dataset.title,b.dataset.mclass,b.dataset.memoji,b.dataset.state);"
        "});"
        "refreshOpenModal();"
        "})();"
    )


def pipeline_timeline_component_height(
    snapshot: PipelineSnapshot, *, show_completion_hero: bool = False
) -> int:
    """Altura del iframe del timeline (debe coincidir con el panel de buckets debajo)."""
    n_stages = len(snapshot.stages)
    cards_rows = max(1, -(-n_stages // 5))
    extra = 60 if show_completion_hero else 0
    return 135 + cards_rows * 92 + extra


def render_pipeline_timeline(
    snapshot: PipelineSnapshot,
    *,
    show_completion_hero: bool = False,
    tracked_manual_run_id: str | None = None,
    ui_locked: bool = False,
) -> None:
    tracks       = pipeline_tracks_manual_run(snapshot, tracked_manual_run_id)
    progress_pct = int(snapshot.overall_progress * 100) if tracks else 0
    # Sin progreso: bolita al inicio del track (antes min 2 % empujaba el indicador a la derecha).
    object_left  = min(max(progress_pct, 0), 100)

    pipeline_html = _build_html(
        snapshot=snapshot,
        progress_pct=progress_pct,
        object_left=object_left,
        show_completion_hero=show_completion_hero,
        ui_locked=ui_locked,
    )

    height = pipeline_timeline_component_height(snapshot, show_completion_hero=show_completion_hero)

    components.html(pipeline_html, height=height, scrolling=False)