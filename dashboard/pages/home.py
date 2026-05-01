"""
Página de Portada del Dashboard Green AI.
Muestra KPIs generales y overview de la plataforma.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Importar conexión S3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from s3_connection import load_fact_table, load_dimension


def render():
    """Renderizar página de portada."""
    
    st.markdown("""
    <style>
    .main-title {
        font-size: 2.5rem;
        font-weight: bold;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }
    .subtitle {
        font-size: 1.2rem;
        color: #ffffff;
        margin-bottom: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<p class="main-title">🌱 Green AI Analytics Platform</p>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Dashboard de Sostenibilidad para Cargas de IA en la Nube</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Intentar cargar datos para KPIs
    try:
        # Cargar datos de ejemplo o datos reales
        df_usage = load_fact_table("fact_ai_compute_usage")
        df_carbon = load_fact_table("fact_carbon_intensity_hourly")
        df_country = load_dimension("dim_country")
        
        # Si no hay datos reales, usar datos de ejemplo
        if df_usage.empty and df_carbon.empty:
            st.info("📌 Mostrando datos de demostración. Configure las credenciales AWS para cargar datos reales.")
            df_usage = get_demo_usage_data()
            df_carbon = get_demo_carbon_data()
            df_country = get_demo_country_data()
    except Exception as e:
        st.warning(f"Usando datos de demostración: {e}")
        df_usage = get_demo_usage_data()
        df_carbon = get_demo_carbon_data()
        df_country = get_demo_country_data()
    
    # KPIs principales
    col1, col2, col3, col4 = st.columns(4)
    
    total_energy = df_usage['energy_consumed_kwh'].sum() if not df_usage.empty else 125000
    total_emissions = total_energy * 0.35  # Factor promedio
    total_cost = df_usage['cost_electricity_usd'].sum() if not df_usage.empty else 45000
    countries_count = len(df_country) if not df_country.empty else 45
    
    with col1:
        st.metric(
            label="⚡ Energía Total Analizada",
            value=f"{total_energy:,.0f} kWh",
            delta=None
        )
    
    with col2:
        st.metric(
            label="🌍 Emisiones Estimadas",
            value=f"{total_emissions:,.0f} tCO₂eq",
            delta=None
        )
    
    with col3:
        st.metric(
            label="💵 Costo Eléctrico Estimado",
            value=f"${total_cost:,.0f}",
            delta=None
        )
    
    with col4:
        st.metric(
            label="🌎 Países/Regiones Cubiertos",
            value=str(countries_count),
            delta=None
        )
    
    st.markdown("---")
    
    # Gráficos de overview
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("📊 Distribución por Tipo de Carga")
        if not df_usage.empty:
            job_type_dist = df_usage.groupby('job_type').agg({
                'energy_consumed_kwh': 'sum',
                'session_id': 'count'
            }).reset_index()
            job_type_dist.columns = ['job_type', 'energy_kwh', 'sessions']
            
            fig = px.bar(
                job_type_dist, 
                x='job_type', 
                y='energy_kwh',
                color='job_type',
                title="Energía por Tipo de Job",
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No hay datos de uso de cómputo disponibles")
    
    with col_right:
        st.subheader("🌍 Intensidad de Carbono por Región")
        if not df_carbon.empty:
            carbon_by_region = df_carbon.groupby('zone').agg({
                'carbon_intensity_gco2eq_per_kwh': 'mean'
            }).reset_index()
            carbon_by_region.columns = ['zone', 'avg_carbon_intensity']
            carbon_by_region = carbon_by_region.sort_values('avg_carbon_intensity', ascending=True).head(10)
            
            fig = px.bar(
                carbon_by_region,
                x='avg_carbon_intensity',
                y='zone',
                orientation='h',
                title="Intensidad de Carbono (gCO₂eq/kWh)",
                color='avg_carbon_intensity',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No hay datos de intensidad de carbono disponibles")
    
    # Mapa o ranking de países
    st.markdown("---")
    st.subheader("🌐 Ranking de Países por Intensidad de Carbono")
    
    if not df_country.empty:
        # Crear datos de ranking
        ranking_data = pd.DataFrame({
            'country': ['Noruega', 'Suecia', 'Canadá', 'Brasil', 'México', 'Argentina', 'Chile', 'Colombia', 'Perú', 'EEUU'],
            'carbon_intensity': [20, 35, 45, 55, 380, 210, 320, 180, 290, 400],
            'low_carbon_share': [98, 95, 92, 78, 45, 55, 40, 65, 50, 35]
        })
        
        fig = px.scatter(
            ranking_data,
            x='country',
            y='carbon_intensity',
            size='low_carbon_share',
            color='carbon_intensity',
            color_continuous_scale='Greens',
            title="Países por Intensidad de Carbono y % Energía Limpia",
            labels={'carbon_intensity': 'Intensidad de Carbono (gCO₂eq/kWh)', 
                   'country': 'País',
                   'size': '% Energía Baja en Carbono'}
        )
        st.plotly_chart(fig, width='stretch')
    
    # Información de metodología
    st.markdown("---")
    st.info("""
    ℹ️ **Nota:** Este dashboard se conecta al bucket S3 `green-ai-pf-gold-a0e96d06` 
    para cargar datos de la capa Gold. Configure las credenciales AWS en el archivo `.env`.
    """)


def get_demo_usage_data():
    """Generar datos de demostración para uso de cómputo."""
    return pd.DataFrame({
        'session_id': [f'session_{i}' for i in range(100)],
        'job_type': ['Training'] * 40 + ['Inference'] * 35 + ['Fine-tuning'] * 25,
        'execution_status': ['Success'] * 80 + ['Failed'] * 20,
        'region': ['us-east-1'] * 30 + ['us-west-2'] * 25 + ['eu-west-1'] * 25 + ['ap-southeast-1'] * 20,
        'gpu_model': ['A100'] * 35 + ['H100'] * 30 + ['V100'] * 20 + ['T4'] * 15,
        'energy_consumed_kwh': [100 + i * 10 for i in range(100)],
        'cost_electricity_usd': [50 + i * 5 for i in range(100)],
        'duration_hours': [1 + i * 0.1 for i in range(100)]
    })


def get_demo_carbon_data():
    """Generar datos de demostración para intensidad de carbono."""
    zones = ['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'AR', 'CL', 'CO', 'DE', 'FR']
    data = []
    for zone in zones:
        for hour in range(24):
            data.append({
                'zone': zone,
                'hour': hour,
                'carbon_intensity_gco2eq_per_kwh': 50 + (hour % 12) * 30 + (hash(zone) % 100)
            })
    return pd.DataFrame(data)


def get_demo_country_data():
    """Generar datos de demostración para países."""
    return pd.DataFrame({
        'country_id': ['USA', 'CAN', 'MEX', 'BRA', 'ARG', 'CHL', 'COL', 'PER'],
        'country_name': ['Estados Unidos', 'Canadá', 'México', 'Brasil', 'Argentina', 'Chile', 'Colombia', 'Perú'],
        'iso_alpha3': ['USA', 'CAN', 'MEX', 'BRA', 'ARG', 'CHL', 'COL', 'PER'],
        'region': ['Norteamérica', 'Norteamérica', 'Latinoamérica', 'Latinoamérica', 'Latinoamérica', 'Latinoamérica', 'Latinoamérica', 'Latinoamérica'],
        'income_group': ['Alto', 'Alto', 'Medio-Alto', 'Medio-Alto', 'Medio-Alto', 'Alto', 'Medio', 'Medio']
    })


if __name__ == "__main__":
    render()