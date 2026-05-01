"""
Un solo iframe con timeline + inventario S3 (evita el hueco grande entre dos components.html).
"""

from __future__ import annotations

import streamlit.components.v1 as components

from components.data_lake_buckets import (
    _BUCKETS_EMBED_CSS,
    _BUCKETS_SCROLL_SCRIPT,
    build_buckets_shell_html,
)
from components.pipeline_timeline import (
    _get_css,
    build_timeline_stack_html_and_script,
    pipeline_timeline_component_height,
    pipeline_tracks_manual_run,
)
from utils.state_schema import PipelineSnapshot

_STACK_GAP_PX = 10


def render_pipeline_live_stack(
    snapshot: PipelineSnapshot,
    *,
    show_completion_hero: bool,
    tracked_manual_run_id: str | None,
    ui_locked: bool,
    bucket_catalog: dict[str, list[str]],
    bucket_errors: dict[str, str | None],
) -> None:
    tracks = pipeline_tracks_manual_run(snapshot, tracked_manual_run_id)
    progress_pct = int(snapshot.overall_progress * 100) if tracks else 0
    object_left = min(max(progress_pct, 0), 100)

    shell_t, script_t = build_timeline_stack_html_and_script(
        snapshot,
        progress_pct,
        object_left,
        show_completion_hero,
        ui_locked=ui_locked,
    )
    shell_b = build_buckets_shell_html(bucket_catalog, bucket_errors)

    timeline_h = pipeline_timeline_component_height(
        snapshot, show_completion_hero=show_completion_hero
    )
    buckets_h = timeline_h
    total_h = timeline_h + _STACK_GAP_PX + buckets_h

    stack_css = f"""
html, body {{
  margin: 0;
  padding: 0;
  height: 100%;
  overflow: hidden;
  background: transparent;
}}
body {{
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
}}
.ct-pl-stack {{
  flex: 1;
  display: flex;
  flex-direction: column;
  width: 100%;
  box-sizing: border-box;
  gap: {_STACK_GAP_PX}px;
  min-height: 0;
  align-items: stretch;
  align-content: flex-start;
  justify-content: flex-start;
  margin: 0;
  padding: 0;
}}
.ct-pl-timeline {{
  flex: 0 0 auto;
  width: 100%;
  max-height: {timeline_h}px;
  height: fit-content;
  margin: 0;
  padding: 0;
  overflow: hidden;
  display: block;
}}
.ct-pl-timeline .pipeline-shell {{
  margin-bottom: 0;
  padding-bottom: 0.55rem;
}}
.ct-pl-buckets {{
  flex: 1 1 auto;
  min-height: {buckets_h}px;
  height: auto;
  width: 100%;
  min-width: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}}
.ct-pl-buckets .dlb-shell {{
  flex: 1 1 auto;
  min-height: 0;
  height: auto !important;
  max-height: none !important;
}}
"""

    doc = (
        "<!DOCTYPE html>"
        '<html lang="es">'
        "<head>"
        '<meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<style>"
        + _get_css()
        + _BUCKETS_EMBED_CSS
        + stack_css
        + "</style>"
        "</head>"
        "<body>"
        '<div class="ct-pl-stack">'
        f'<div class="ct-pl-timeline">{shell_t}</div>'
        f'<div class="ct-pl-buckets">{shell_b}</div>'
        "</div>"
        + script_t
        + "<script>"
        + _BUCKETS_SCROLL_SCRIPT
        + "</script>"
        "</body>"
        "</html>"
    )

    components.html(doc, height=total_h, scrolling=False)
