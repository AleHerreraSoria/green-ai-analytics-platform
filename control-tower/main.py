"""
Landing del Control Tower: storytelling, visión de la ONG y resumen de impacto.
Sin lógica de ingesta ni credenciales; estado técnico alineado con README del repo.
"""

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR = Path(__file__).resolve().parent
ASSETS = BASE_DIR / "assets"
# Video educativo (embebido); sustituye el ID por material propio del equipo si lo preferís.
YOUTUBE_EMBED_ID = "1kUE0BZtTRc"  # National Geographic — Renewable Energy 101 (contexto energético)
HERO_IMAGE = (
    "https://images.unsplash.com/photo-1473341304170-971dccb5ac1e"
    "?auto=format&fit=crop&w=1600&q=80"
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
          .block-container { padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1180px; }
          div[data-testid="stExpander"] details summary p {
            font-weight: 600;
            letter-spacing: 0.02em;
          }
          .ga-hero {
            background: radial-gradient(1200px 500px at 10% -10%, rgba(222,255,154,0.22), transparent 55%),
                        radial-gradient(900px 400px at 90% 0%, rgba(222,255,154,0.12), transparent 50%),
                        linear-gradient(135deg, #151515 0%, #0b0b0b 100%);
            border: 1px solid rgba(222,255,154,0.25);
            border-radius: 18px;
            padding: 1.75rem 1.75rem 1.5rem 1.75rem;
            margin-bottom: 1.25rem;
          }
          .ga-hero h1 {
            font-size: clamp(1.75rem, 2.6vw, 2.35rem);
            line-height: 1.15;
            margin: 0 0 0.5rem 0;
            color: #f7fff0;
          }
          .ga-hero p.lead {
            color: #d6d6d6;
            font-size: 1.05rem;
            max-width: 52rem;
            margin: 0;
          }
          .ga-pill {
            display: inline-block;
            padding: 0.35rem 0.75rem;
            border-radius: 999px;
            border: 1px solid rgba(222,255,154,0.45);
            color: #111;
            background: #deff9a;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-bottom: 0.75rem;
          }
          .ga-card {
            background: #161616;
            border: 1px solid #2a2a2a;
            border-radius: 14px;
            padding: 1rem 1.1rem;
            height: 100%;
          }
          .ga-card h3 { margin: 0 0 0.35rem 0; font-size: 1.05rem; color: #deff9a; }
          .ga-card p { margin: 0; color: #cfcfcf; font-size: 0.92rem; line-height: 1.45; }
          .ga-foot {
            color: #9a9a9a;
            font-size: 0.85rem;
            margin-top: 2rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def youtube_embed(video_id: str, height: int = 420) -> None:
    src = f"https://www.youtube-nocookie.com/embed/{video_id}"
    components.html(
        f"""
        <div style="position:relative;width:100%;padding-bottom:56.25%;height:0;overflow:hidden;border-radius:12px;
                    box-shadow:0 12px 40px rgba(0,0,0,0.45);border:1px solid rgba(222,255,154,0.25);">
          <iframe style="position:absolute;top:0;left:0;width:100%;height:100%;"
            src="{src}"
            title="Contexto: energía y transición (video educativo)"
            frameborder="0"
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
            allowfullscreen>
          </iframe>
        </div>
        """,
        height=height,
    )


st.set_page_config(
    page_title="Green AI — Control Tower",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🌿",
)

inject_styles()

st.sidebar.markdown("### Green AI Analytics Platform")
st.sidebar.caption(
    "PoC de plataforma de datos abiertos para analizar impacto ambiental y "
    "económico de la IA en América (lakehouse Medallion en S3)."
)
st.sidebar.divider()
st.sidebar.markdown("**Navegación**")
st.sidebar.info(
    "Home Storytelling + página `Pipeline Live` para seguimiento en tiempo real."
)

hero = """
<div class="ga-hero">
  <div class="ga-pill">Data Engineering · Sostenibilidad digital</div>
  <h1>Green AI Analytics Platform</h1>
  <p class="lead">
    Integración de energía histórica, intensidad de carbono (casi) en tiempo real,
    catálogo de hardware y logs sintéticos de sesiones de cómputo para visibilizar
    cómo la <strong>movilidad de datos</strong> puede reducir emisiones y coste operativo
    en el continente americano.
  </p>
</div>
"""
st.markdown(hero, unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Capa Medallion", "Bronze activa", "Silver/Gold en roadmap")
with c2:
    st.metric("Orquestación", "Airflow 2.8 (Compose)", "DAGs: pendientes en repo")
with c3:
    st.metric("Infraestructura", "Terraform + S3", "EC2 según IaC")
with c4:
    st.metric("Visualización", "Streamlit (PoC)", "Power BI: opcional")

st.divider()

left, right = st.columns((1.05, 1), gap="large")

with left:
    st.subheader("Caso de negocio")
    st.write(
        "Una ONG con presencia en América necesita una prueba de concepto que "
        "consolide fuentes heterogéneas (CSV, API, logs) en un **data lake** con "
        "trazabilidad y un camino claro hacia tablas analíticas."
    )
    st.write(
        "El foco no es solo reportar consumo, sino **comparar regiones y ventanas "
        "horarias** donde la red eléctrica es más limpia, y relacionar hardware, "
        "kWh y emisiones para decisiones data-driven."
    )
    with st.expander("Objetivo técnico alineado con el repositorio", expanded=False):
        st.markdown(
            """
            - **Arquitectura Medallion** (Bronze / Silver / Gold) sobre **Amazon S3**.
            - **Ingesta híbrida**: batch (Our World in Data, MLCO2, referencias) + API (Electricity Maps) + generador de logs.
            - **Orquestación** con **Apache Airflow** en Docker para desarrollo local.
            - **Procesamiento** previsto con **PySpark** hacia capas curadas y modelo dimensional estilo Kimball.
            """
        )

with right:
    st.subheader("Mapa mental del producto")
    st.image(HERO_IMAGE, use_container_width=True, caption="Progreso tecnológico y presión sobre la red eléctrica regional.")
    
st.divider()

st.subheader("Flujo del pipeline")
st.image(str(ASSETS / "medallion-flow.svg"), use_container_width=True)

st.divider()

st.subheader("Fuentes de datos")
ftab1, ftab2 = st.tabs(["Resumen ejecutivo", "Detalle técnico"])

with ftab1:
    fc1, fc2, fc3, fc4 = st.columns(4)
    cards = [
        (
            "Our World in Data",
            "Contexto histórico: energía, economía y demografía por país. CSV local bajo `data/` (no versionado).",
        ),
        (
            "Electricity Maps",
            "Intensidad de carbono y señales de red; ingesta vía API (`scripts/ingest_electricity_maps.py`).",
        ),
        (
            "MLCO2 / CodeCarbon",
            "Hardware y TDP para traducir sesiones de uso en kWh/emisiones. CSV local esperado en `data/`.",
        ),
        (
            "Logs sintéticos",
            "Simulación de movilidad regional de cargas (`scripts/generate_synthetic_usage_logs.py` → `bronze/`).",
        ),
    ]
    cols = [fc1, fc2, fc3, fc4]
    for col, (title, body) in zip(cols, cards):
        with col:
            st.markdown(
                f'<div class="ga-card"><h3>{title}</h3><p>{body}</p></div>',
                unsafe_allow_html=True,
            )

with ftab2:
    st.markdown(
        """
        | Fuente | Rol | En el repositorio |
        |--------|-----|-------------------|
        | Our World in Data | Mezcla energética e indicadores | Rutas esperadas por `upload_bronze_to_s3.py` |
        | Electricity Maps | Tiempo (casi) real | JSON bajo prefijo `electricity_maps/` |
        | MLCO2 | Dimensión hardware | Alineado al diccionario en `docs/DICCIONARIO_DE_DATOS.md` |
        | Logs sintéticos | Hechos de uso / movilidad | `bronze/usage_logs/usage_logs.csv` |
        """
    )
    
st.divider()

vcol1, vcol2 = st.columns((1.15, 1), gap="large")

with vcol1:
    st.subheader("Video de contexto")
    youtube_embed(YOUTUBE_EMBED_ID, height=400)

with vcol2:
    st.subheader("Interactividad ligera")
    st.write(
        "Esta pieza es solo **storytelling**: ilustra la idea de aplazar o mover cargas "
        "cuando la red está más limpia, sin conectar aún a datos reales."
    )
    hours = st.slider("Ventana horaria preferente para cómputo intensivo (h)", 2, 24, 8)
    intensity = st.select_slider(
        "Intensidad de carbono de la red (gCO₂eq/kWh, ilustrativo)",
        options=[120, 220, 320, 420, 520, 650],
        value=320,
    )
    baseline = intensity * hours * 0.0012
    greener = max(baseline * 0.72, 0.01)
    st.metric(
        "Emisiones evitadas (ejemplo normalizado)",
        f"{baseline - greener:.2f} tCO₂e (demo)",
        delta=f"-{(1 - greener / baseline) * 100:.0f}% vs. escenario base",
    )
   
st.divider()

st.subheader("Preguntas de negocio (prioridad README)")
pq1, pq2 = st.columns(2)
with pq1:
    st.markdown(
        """
        - Optimización regional y **“horas verdes”** al mover entrenamiento entre países.
        - Eficiencia de **hardware** frente a coste eléctrico y huella.
        """
    )
with pq2:
    st.markdown(
        """
        - Correlación **PIB / desarrollo** vs limpieza de la red.
        - **Proyecciones** de demanda energética ante crecimiento de adopción de IA.
        """
    )

st.divider()

st.subheader("Equipo")
st.markdown(
    """
    - Alejandro Herrera — Data Engineer  
    - José Frías — Data Engineer  
    - Nuri Naranjo — Data Engineer  
    - Adrián Velázquez — Data Engineer  
    - Eduardo Cárdenas — Scrum Master  
    """
)

st.markdown(
    '<p class="ga-foot">Green AI Analytics Platform — proyecto académico Data Engineering. '
    "Revisá licencias de datasets externos antes de redistribuir datos o derivados.</p>",
    unsafe_allow_html=True,
)
