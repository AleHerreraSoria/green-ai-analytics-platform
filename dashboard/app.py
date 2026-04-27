"""
Green AI Analytics Platform - Dashboard Streamlit
===================================================
Dashboard para analizar la sostenibilidad de cargas de IA en la nube.

Estructura basada en Propuesta_para_Dashboard_Green_AI.md
"""
import streamlit as st
import os
import sys
from dotenv import load_dotenv

# Agregar src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Cargar configuración
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Configuración de página
st.set_page_config(
    page_title="Green AI Analytics",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados
st.markdown("""
<style>
    /* =========================
       Fondo y texto general
       ========================= */
    html, body, .stApp,
    div[data-testid="stAppViewContainer"],
    div[data-testid="stMain"] {
        background-color: #003817 !important;
        color: #ffffff !important;
    }

    .main,
    .block-container,
    section.main,
    div[data-testid="stVerticalBlock"] {
        background-color: #003817 !important;
        color: #ffffff !important;
    }

    .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
    .stApp p, .stApp li, .stApp label,
    .stApp div[data-testid="stMarkdownContainer"],
    .stApp div[data-testid="stMarkdownContainer"] *,
    .stApp div[data-testid="stHeading"],
    .stApp div[data-testid="stHeading"] *,
    .stApp div[data-testid="stText"],
    .stApp div[data-testid="stText"] * {
        color: #ffffff !important;
    }

    /* =========================
       Sidebar
       ========================= */
    section[data-testid="stSidebar"] {
        background-color: #002B12 !important;
    }

    section[data-testid="stSidebar"],
    section[data-testid="stSidebar"] * {
        color: #ffffff !important;
    }

    /* =========================
       Headers personalizados
       ========================= */
    .page-header {
        font-size: 1.8rem;
        font-weight: bold;
        margin-bottom: 1rem;
        color: #ffffff !important;
    }

    .page-subheader {
        font-size: 1rem;
        color: #ffffff !important;
        margin-bottom: 1.5rem;
    }

    /* =========================
       KPIs / métricas
       ========================= */
    .metric-card {
        background-color: rgba(255, 255, 255, 0.06) !important;
        padding: 1rem;
        border-radius: 0.75rem;
        border-left: 4px solid #10b981;
    }

    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #ffffff !important;
    }

    .kpi-label {
        font-size: 0.9rem;
        color: #ffffff !important;
    }

    div[data-testid="stMetric"] {
        background-color: rgba(255, 255, 255, 0.06) !important;
        border: 1px solid rgba(255, 255, 255, 0.15) !important;
        border-radius: 12px !important;
        padding: 12px !important;
    }

    div[data-testid="stMetric"] * {
        color: #ffffff !important;
    }

    /* =========================
       Tablas y alertas
       ========================= */
    table, thead, tbody, tr, th, td {
        color: #ffffff !important;
        border-color: rgba(255, 255, 255, 0.35) !important;
    }

    th {
        background-color: rgba(255, 255, 255, 0.14) !important;
        font-weight: 700 !important;
    }

    td {
        background-color: rgba(255, 255, 255, 0.04) !important;
    }

    div[data-testid="stAlert"],
    div[data-testid="stAlert"] * {
        color: #ffffff !important;
    }

    div[data-testid="stAlert"] {
        background-color: rgba(16, 185, 129, 0.18) !important;
        border-radius: 12px !important;
    }

    hr {
        border-color: rgba(255, 255, 255, 0.25) !important;
    }

    code {
        color: #d1fae5 !important;
        background-color: rgba(255, 255, 255, 0.14) !important;
        border-radius: 4px;
        padding: 2px 4px;
    }

    /* =========================
       Selectbox / desplegables
       Importante: fondo claro con texto negro
       ========================= */
    div[data-testid="stSelectbox"] label {
        color: #ffffff !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
    div[data-baseweb="select"] > div {
        background-color: #f8fafc !important;
        border: 1px solid #cbd5e1 !important;
        color: #111827 !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] *,
    div[data-baseweb="select"] span,
    div[data-baseweb="select"] input {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] svg,
    div[data-baseweb="select"] svg {
        fill: #111827 !important;
        color: #111827 !important;
    }

    div[data-baseweb="popover"] {
        background-color: #ffffff !important;
        color: #111827 !important;
    }

    div[data-baseweb="popover"] *,
    div[role="listbox"] *,
    div[role="option"] * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }

    div[role="option"] {
        background-color: #ffffff !important;
        color: #111827 !important;
    }

    div[role="option"]:hover {
        background-color: #d1fae5 !important;
        color: #111827 !important;
    }

    /* Radio y otros filtros: mantener blanco sobre fondo oscuro */
    div[data-testid="stRadio"] *,
    div[role="radiogroup"] * {
        color: #ffffff !important;
    }

    /* =========================
       Botones
       ========================= */
    .stButton > button {
        background-color: #10B981 !important;
        color: #ffffff !important;
        border-radius: 10px !important;
        border: none !important;
    }

    .stButton > button:hover {
        background-color: #059669 !important;
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)



def main():
    """Página principal con navegación."""
    
    # Sidebar con navegación
    st.sidebar.title("🌱 Green AI")
    st.sidebar.markdown("---")
    
    # Menú de navegación
    pages = {
        "🏠 Portada": "home",
        "📊 Página 1: Movilidad regional": "page1",
        "💻 Página 2: GPU para la ONG": "page2",
        "💰 Página 3: Costo de carbono por TFLOP": "page3",
        "🌐 Página 4: Exportaciones TIC": "page4",
        "💵 Página 5: PIB per cápita (América)": "page5",
        "📈 Página 6: Escenario +20% Latam": "page6",
        "⏰ Página 7: Ventanas operativas": "page7",
        "⚠️ Página 8: Puntos ciegos": "page8",
        "🔄 Página 9: Tipo de carga vs resultado": "page9",
        "📏 Página 10: Escala vs sostenibilidad": "page10",
        "📖 Metodología": "methodology"
    }
    
    selected_page = st.sidebar.radio(
        "Navegación",
        list(pages.keys()),
        format_func=lambda x: x
    )
    
    # Selector global en sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("Filtros Globales")
    
    year_filter = st.sidebar.selectbox(
        "Año",
        options=["Todos", "2024", "2023", "2022"],
        index=0
    )
    
    region_filter = st.sidebar.selectbox(
        "Región",
        options=["Todas", "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
        index=0
    )
    
    # Almacenar filtros en session state
    st.session_state['year_filter'] = year_filter
    st.session_state['region_filter'] = region_filter
    
    # Renderizar página seleccionada
    page_key = pages[selected_page]
    
    if page_key == "home":
        from pages import home
        home.render()
    elif page_key == "page1":
        from pages import page1_movilidad_regional
        page1_movilidad_regional.render()
    elif page_key == "page2":
        from pages import page2_gpu_ong
        page2_gpu_ong.render()
    elif page_key == "page3":
        from pages import page3_costo_carbono_tflop
        page3_costo_carbono_tflop.render()
    elif page_key == "page4":
        from pages import page4_exportaciones_tic
        page4_exportaciones_tic.render()
    elif page_key == "page5":
        from pages import page5_pib_america
        page5_pib_america.render()
    elif page_key == "page6":
        from pages import page6_escenario_latam
        page6_escenario_latam.render()
    elif page_key == "page7":
        from pages import page7_ventanas_verdes
        page7_ventanas_verdes.render()
    elif page_key == "page8":
        from pages import page8_puntos_ciegos
        page8_puntos_ciegos.render()
    elif page_key == "page9":
        from pages import page9_tipo_carga_resultado
        page9_tipo_carga_resultado.render()
    elif page_key == "page10":
        from pages import page10_escala_sostenibilidad
        page10_escala_sostenibilidad.render()
    elif page_key == "methodology":
        from pages import methodology
        methodology.render()


if __name__ == "__main__":
    main()