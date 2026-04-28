"""
Panel de los tres buckets (bronze / silver / gold) con listado de claves S3.
"""

from __future__ import annotations

import html

import streamlit.components.v1 as components

_TIER_META = (
    ("bronze", "Bronze", "medal-bronze", "🥉"),
    ("silver", "Silver", "medal-silver", "🥈"),
    ("gold", "Gold", "medal-gold", "🥇"),
)

# Mismo JS que el iframe standalone (scroll al final de cada columna).
_BUCKETS_SCROLL_SCRIPT = """
(function(){
  function scrollListsToBottom(){
    document.querySelectorAll('.dlb-col-body').forEach(function(el){
      el.scrollTop = el.scrollHeight;
    });
  }
  scrollListsToBottom();
  requestAnimationFrame(function(){
    scrollListsToBottom();
    requestAnimationFrame(scrollListsToBottom);
  });
})();
"""

# CSS del panel sin reglas globales html/body (para documento combinado con el timeline).
_BUCKETS_EMBED_CSS = """
*, *::before, *::after { box-sizing: border-box; }
.dlb-shell {
  background: linear-gradient(160deg, #121212 0%, #0c0c0c 100%);
  border: 1px solid rgba(222,255,154,.2);
  border-radius: 16px;
  padding: 1rem 1.1rem 1.1rem;
  box-sizing: border-box;
  height: 100%;
  max-height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.dlb-top {
  display: flex; align-items: baseline; justify-content: space-between; gap: .75rem;
  flex-wrap: wrap; margin-bottom: .65rem;
}
.dlb-top h3 {
  color: #f0ffe0; font-size: 1rem; font-weight: 700; letter-spacing: .01em;
  margin: 0;
}
.dlb-hint {
  font-size: .72rem; color: rgba(200,220,180,.55); font-weight: 500;
}
.dlb-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: .45rem;
  align-items: stretch;
  flex: 1 1 0;
  min-height: 0;
  overflow: hidden;
}
@media (max-width: 720px) {
  .dlb-grid { grid-template-columns: 1fr; }
}
.dlb-col {
  background: #181818;
  border: 1px solid #282828;
  border-radius: 10px;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.dlb-col-head {
  display: flex;
  align-items: center;
  gap: .4rem;
  padding: .45rem .5rem;
  border-bottom: 1px solid #242424;
}
.dlb-medal {
  width: 1.75rem; height: 1.75rem; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: .88rem; line-height: 1;
  border: 1px solid rgba(255,255,255,.1);
  flex-shrink: 0;
}
.medal-bronze { background: radial-gradient(circle at 32% 28%, #e8a060, #8b4513 58%, #4a2509 100%); }
.medal-silver { background: radial-gradient(circle at 32% 28%, #f2f2f2, #a8aeb8 55%, #5c636b 100%); }
.medal-gold   { background: radial-gradient(circle at 32% 28%, #fff4b8, #e8c547 50%, #a67c00 100%); }
.dlb-tier-title { color: #e8e8e8; font-size: .78rem; font-weight: 600; margin: 0; }
.dlb-tier-sub { color: #888; font-size: .65rem; margin: .1rem 0 0 0; }
.dlb-col-body {
  flex: 1 1 0;
  padding: .4rem .45rem .5rem;
  min-height: 0;
  overflow-x: hidden;
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: thin;
  scrollbar-color: rgba(222,255,154,.42) rgba(30,30,30,.9);
}
.dlb-col-body::-webkit-scrollbar {
  width: 9px;
}
.dlb-col-body::-webkit-scrollbar-track {
  background: rgba(30,30,30,.85);
  border-radius: 6px;
}
.dlb-col-body::-webkit-scrollbar-thumb {
  background: rgba(158,220,104,.38);
  border-radius: 6px;
}
.dlb-col-body::-webkit-scrollbar-thumb:hover {
  background: rgba(222,255,154,.48);
}
.dlb-empty {
  color: #777; font-size: .68rem; line-height: 1.35; margin: .2rem 0 0 0;
}
.dlb-error { color: #ff9a8a; }
.dlb-list {
  list-style: none; margin: 0; padding: 0;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: .62rem;
  line-height: 1.38;
  color: #c8e8a8;
}
.dlb-key {
  padding: .1rem 0 .1rem .15rem;
  border-bottom: 1px solid rgba(255,255,255,.04);
  animation: dlb-in .45s ease;
}
.dlb-dot { opacity: .45; margin-right: .2rem; }
@keyframes dlb-in {
  from { opacity: 0; transform: translateY(3px); }
  to { opacity: 1; transform: translateY(0); }
}
"""


def build_buckets_shell_html(
    bucket_catalog: dict[str, list[str]],
    bucket_errors: dict[str, str | None],
) -> str:
    """Solo el div.dlb-shell (para empilar con el timeline en un único iframe)."""
    cols_html_parts: list[str] = []
    for tier_id, title, medal_cls, emoji in _TIER_META:
        keys = bucket_catalog.get(tier_id) or []
        err = bucket_errors.get(tier_id)
        title_e = html.escape(title)
        emoji_e = html.escape(emoji)

        if err:
            body_inner = f'<p class="dlb-empty dlb-error">{html.escape(err)}</p>'
        elif not keys:
            body_inner = '<p class="dlb-empty">Esperando carga de archivos...</p>'
        else:
            lines = "".join(
                f'<li class="dlb-key"><span class="dlb-dot">·</span>{html.escape(k)}</li>'
                for k in keys
            )
            body_inner = f'<ul class="dlb-list">{lines}</ul>'

        cols_html_parts.append(
            f'<div class="dlb-col">'
            f'<div class="dlb-col-head">'
            f'<span class="dlb-medal {medal_cls}" aria-hidden="true">{emoji_e}</span>'
            f'<div><p class="dlb-tier-title">{title_e}</p>'
            f'<p class="dlb-tier-sub">{len(keys)} objeto(s)</p></div>'
            f"</div>"
            f'<div class="dlb-col-body">{body_inner}</div>'
            f"</div>"
        )

    cols_joined = "".join(cols_html_parts)
    return (
        f'<div class="dlb-shell">'
        f'<div class="dlb-top"><h3>Data lake — inventario S3</h3>'
        f'<span class="dlb-hint">Se actualiza con cada etapa del pipeline</span></div>'
        f'<div class="dlb-grid">{cols_joined}</div>'
        f"</div>"
    )


def render_data_lake_buckets(
    *,
    bucket_catalog: dict[str, list[str]],
    bucket_errors: dict[str, str | None],
    height_px: int,
) -> None:
    """Iframe solo con buckets (el layout Pipeline Live usa el stack combinado)."""
    shell = build_buckets_shell_html(bucket_catalog, bucket_errors)
    css = (
        "html, body { height: 100%; margin: 0; overflow: hidden; box-sizing: border-box; }\n"
        + _BUCKETS_EMBED_CSS
    )
    doc = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<style>{css}</style></head><body style='margin:0;background:transparent;height:100%;overflow:hidden;'>"
        f"{shell}"
        f"<script>{_BUCKETS_SCROLL_SCRIPT}</script>"
        "</body></html>"
    )

    components.html(doc, height=height_px, scrolling=False)
