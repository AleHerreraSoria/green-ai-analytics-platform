"""
Seccion integrada del dashboard analitico (`control-tower/analytics_dashboard/`).
Mantiene intacta la app actual de control-tower y agrega una nueva pagina.
"""

from __future__ import annotations

import importlib.util
import os
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


def _inject_dashboard_styles() -> None:
    """Aplica estilos base del dashboard original solo para esta pagina."""
    st.markdown(
        """
        <style>
            html, body, .stApp,
            div[data-testid="stAppViewContainer"],
            div[data-testid="stMain"] {
                background-color: #003817 !important;
                color: #ffffff !important;
            }

            .main, .block-container, section.main,
            div[data-testid="stVerticalBlock"] {
                background-color: #003817 !important;
                color: #ffffff !important;
            }

            section[data-testid="stSidebar"] {
                background-color: #002B12 !important;
            }

            section[data-testid="stSidebar"],
            section[data-testid="stSidebar"] * {
                color: #ffffff !important;
            }

            .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
            .stApp p, .stApp li, .stApp label,
            .stApp div[data-testid="stMarkdownContainer"],
            .stApp div[data-testid="stMarkdownContainer"] *,
            .stApp div[data-testid="stHeading"],
            .stApp div[data-testid="stHeading"] * {
                color: #ffffff !important;
            }

            .stButton > button {
                background-color: #10B981 !important;
                color: #ffffff !important;
                border-radius: 10px !important;
                border: none !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    """Render de la seccion Analytics Dashboard dentro de control-tower."""
    ensure_dotenv_loaded()
    load_dotenv(DASHBOARD_DIR / ".env", override=False)

    st.sidebar.title("🌱 Green AI Dashboard")
    st.sidebar.markdown("---")

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

    selected_page = st.sidebar.radio("Navegación dashboard", list(page_map.keys()))

    st.sidebar.markdown("---")
    st.sidebar.subheader("Filtros Globales")
    st.session_state["year_filter"] = st.sidebar.selectbox(
        "Año", options=["Todos", "2024", "2023", "2022"], index=0
    )
    st.session_state["region_filter"] = st.sidebar.selectbox(
        "Región",
        options=["Todas", "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
        index=0,
    )

    _inject_dashboard_styles()

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
