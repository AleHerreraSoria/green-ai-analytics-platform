"""
Página de Metodología del Dashboard Green AI.
Documenta origen de datos, supuestos y unidades.
"""
import streamlit as st

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


def render():
    """Renderizar página de metodología."""
    
    st.markdown('<p class="page-header">📖 Metodología</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Documentación de origen de datos, supuestos y unidades</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== ORIGEN DE DATOS ====================
    st.subheader("📚 Origen de Datos")
    
    st.markdown("""
    ### Fuentes de Datos Utilizadas
    
    | Capa | Tabla | Descripción | Frecuencia |
    |------|-------|-------------|-------------|
    | Gold | `fact_ai_compute_usage` | Sesiones de cómputo de IA con métricas de energía, costo y emisiones | Diaria |
    | Gold | `fact_carbon_intensity_hourly` | Intensidad de carbono horaria por zona eléctrica | Horaria |
    | Gold | `fact_country_energy_annual` | Datos macro de energía por país | Anual |
    | Gold | `dim_country` | Catálogo de países enriquecido con metadatos OWID/World Bank | Estático |
    | Gold | `dim_region` | Mapeo de regiones AWS a zonas Electricity Maps | Estático |
    | Gold | `dim_gpu_model` | Catálogo de GPUs con especificaciones (TDP, TFLOPS) | Estático |
    | Gold | `dim_instance_type` | Catálogo de precios EC2 por región | Estático |
    | Gold | `dim_electricity_price` | Precios eléctricos residenciales por país | Estático |
    """)
    
    st.markdown("---")
    
    # ==================== QUÉ ES REAL Y QUÉ ES PROXY ====================
    st.subheader("🔍 Qué es Real y Qué es Proxy")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### ✅ Datos Reales (Confirmados)
        
        - **Intensidad de carbono horaria**: Datos de Electricity Maps API
        - **Precios EC2**: Precios oficiales de AWS (on-demand)
        - **Precios eléctricos residenciales**: Datos de Global Petrol Prices
        - **Metadatos de países**: Datos de OWID y World Bank
        - **Especificaciones de GPUs**: Datos de MLCO2 (TDP, TFLOPS)
        """)
    
    with col2:
        st.markdown("""
        ### 🔄 Datos Proxy (Estimados)
        
        - **Energía consumida por sesión**: Calculada usando TDP de GPU × duración × utilización
        - **Emisiones de CO₂**: Calculadas usando intensidad de carbono × energía consumida
        - **Costo eléctrico**: Calculado usando precio residencial × energía consumida
        - **Demanda tecnológica**: Proxy basado en exportaciones TIC y adopción de IA
        """)
    
    st.markdown("---")
    
    # ==================== SUPUESTOS TRANSVERSALES ====================
    st.subheader("📐 Supuestos Transversales")
    
    st.markdown("""
    | Supuesto | Valor | Justificación |
    |----------|-------|---------------|
    | Factor de utilización GPU | 80% | Promedio industry para workloads de IA |
    | Factor de emisión grid | Variable por zona/hora | Datos reales de Electricity Maps |
    | Precio eléctrico residencial | Por país | Proxy para costo de energía real |
    | TDP GPU | Según especificaciones | Datos de MLCO2 |
    | Duración de sesión | Del log de uso | Dato real del logs sintético |
    """)
    
    st.markdown("---")
    
    # ==================== UNIDADES ====================
    st.subheader("📏 Unidades Utilizadas")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("⚡ Energía", "kWh")
    
    with col2:
        st.metric("🌍 Emisiones", "tCO₂eq / kgCO₂eq")
    
    with col3:
        st.metric("💵 Costo", "USD / USD/kWh")
    
    with col4:
        st.metric("💻 Rendimiento", "TFLOPS / TFLOP-h")
    
    st.markdown("""
    ### Conversiones Importantes
    
    - **1 tCO₂eq = 1,000 kgCO₂eq**
    - **gCO₂eq/kWh**: Intensidad de carbono por unidad de energía
    - **gCO₂eq/TFLOP-h**: Eficiencia de carbono por unidad de cómputo
    """)
    
    st.markdown("---")
    
    # ==================== NOTAS METODOLÓGICAS ====================
    st.subheader("📝 Notas Metodológicas")
    
    st.info("""
    ### Consideraciones Importantes
    
    1. **Logs Sintéticos**: Los datos de uso de cómputo (`fact_ai_compute_usage`) son generados sintéticamente 
       mediante el script `generate_synthetic_usage_logs.py`. Deben validarse contra logs reales en producción.
    
    2. **Frecuencia de Actualización**: Los datos de intensidad de carbono se actualizan cada hora desde 
       Electricity Maps. Las dimensiones son estáticas y se actualizan mensualmente.
    
    3. **Particionamiento**: Las tablas de hechos están particionadas por `year` y `month` para optimizar 
       consultas y reducir costos de lectura S3.
    
    4. **Surrogate Keys**: Las claves surrogate se generan con MD5 sobre claves naturales normalizadas, 
       garantizando consistencia sin necesidad de secuencias.
    """)
    
    st.markdown("---")
    
    # ==================== BUCKET S3 ====================
    st.subheader("☁️ Configuración S3")
    
    st.markdown(f"""
    ### Conexión al Bucket Gold
    
    - **Bucket**: `green-ai-pf-gold-a0e96d06`
    - **Región**: `us-east-1`
    - **Formato**: Parquet
    
    Para conectar el dashboard a datos reales:
    1. Copiar `.env.example` a `.env`
    2. Configurar credenciales AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
    3. Ejecutar `pip install -r requirements.txt`
    4. Ejecutar `streamlit run app.py`
    """)


if __name__ == "__main__":
    render()