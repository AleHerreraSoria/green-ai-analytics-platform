"""
Seccion integrada del dashboard analitico (`control-tower/analytics_dashboard/`).
Mantiene intacta la app actual de control-tower y agrega una nueva pagina.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from utils.project_env import ensure_dotenv_loaded

BASE_DIR = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = BASE_DIR / "analytics_dashboard"
DASHBOARD_PAGES_DIR = DASHBOARD_DIR / "pages"


def _load_page_module(page_module_name: str):
    """Carga dinamicamente un modulo de pagina del dashboard externo."""
    module_path = DASHBOARD_PAGES_DIR / f"{page_module_name}.py"
    if not module_path.exists():
        raise FileNotFoundError(f"No existe la pagina: {module_path}")

    unique_module_name = f"dashboard_pages_{page_module_name}"
    spec = importlib.util.spec_from_file_location(unique_module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar spec para {page_module_name}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _inject_analytics_section_styles() -> None:
    """Layout y tipografía alineados con `main.py`; respeta el theme global (.streamlit/config.toml)."""
    st.markdown(
        """
        <style>
          /* Misma rejilla que la landing para que el salto entre páginas sea imperceptible */
          .main .block-container {
            padding-top: 1.25rem;
            padding-bottom: 3rem;
            max-width: min(96vw, 1720px);
            padding-left: clamp(1rem, 3vw, 2.5rem);
            padding-right: clamp(1rem, 3vw, 2.5rem);
            font-size: 1.08rem;
          }
          .main h2 { font-size: 1.55rem !important; }
          .main h3 { font-size: 1.2rem !important; }
          .page-header {
            font-size: clamp(1.35rem, 2.2vw, 1.85rem);
            font-weight: 700;
            letter-spacing: 0.02em;
            margin: 0 0 0.35rem 0;
            color: #f7fff0;
          }
          .page-subheader {
            font-size: clamp(1rem, 1.25vw, 1.12rem);
            color: #cfcfcf;
            margin: 0 0 1rem 0;
            line-height: 1.45;
          }
          div[data-testid="stMetricValue"] { font-size: 1.85rem !important; }
          div[data-testid="stMetricLabel"] { font-size: 1rem !important; }
          /* Home del dashboard (clases locales) */
          .ad-home-title {
            font-size: clamp(1.6rem, 2.5vw, 2.35rem);
            font-weight: 700;
            color: #f7fff0;
            margin-bottom: 0.5rem;
          }
          .ad-home-subtitle {
            font-size: clamp(1rem, 1.25vw, 1.18rem);
            color: #d6d6d6;
            margin-bottom: 2rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    """Render de la seccion Analytics Dashboard dentro de control-tower."""
    ensure_dotenv_loaded()
    load_dotenv(DASHBOARD_DIR / ".env", override=False)

    _inject_analytics_section_styles()

    st.sidebar.markdown("### Green AI Analytics Platform")
    st.sidebar.caption(
        "Indicadores y narrativas del modelo analítico; misma shell que Pipeline Live y Home."
    )
    st.sidebar.divider()
    st.sidebar.markdown("**Navegación**")

    page_map = {
        "🏠 Portada": "home",
        "📊 Página 1: Movilidad regional": "page1_movilidad_regional",
        "💻 Página 2: GPU para la ONG": "page2_gpu_ong",
        "💰 Página 3: Costo de carbono por TFLOP": "page3_costo_carbono_tflop",
        "🌐 Página 4: Exportaciones TIC": "page4_exportaciones_tic",
        "💵 Página 5: PIB per cápita (América)": "page5_pib_america",
        "📈 Página 6: Escenario +20% Latam": "page6_escenario_latam",
        "⏰ Página 7: Ventanas operativas": "page7_ventanas_verdes",
        "⚠️ Página 8: Puntos ciegos": "page8_puntos_ciegos",
        "🔄 Página 9: Tipo de carga vs resultado": "page9_tipo_carga_resultado",
        "📏 Página 10: Escala vs sostenibilidad": "page10_escala_sostenibilidad",
        "📖 Metodología": "methodology",
    }

    selected_page = st.sidebar.radio(
        "Sección del dashboard",
        list(page_map.keys()),
        label_visibility="visible",
    )

    st.sidebar.divider()
    st.sidebar.markdown("**Filtros globales**")
    st.session_state["year_filter"] = st.sidebar.selectbox(
        "Año", options=["Todos", "2024", "2023", "2022"], index=0
    )
    st.session_state["region_filter"] = st.sidebar.selectbox(
        "Región",
        options=["Todas", "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
        index=0,
    )

    module_name = page_map[selected_page]
    try:
        module = _load_page_module(module_name)
        module.render()
    except Exception as exc:
        st.error(f"No fue posible cargar la pagina seleccionada: {exc}")
        st.info(
            "Verifica dependencias (`pip install -r requirements.txt`) y variables "
            "de entorno en `.env` en la raíz de control-tower (o en "
            "`analytics_dashboard/.env`) para habilitar la carga de datos desde S3."
        )


main()
