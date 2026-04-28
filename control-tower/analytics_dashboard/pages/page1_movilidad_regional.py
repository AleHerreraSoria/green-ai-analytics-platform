"""
Página 1: Movilidad regional y horas verdes
============================================
Pregunta: ahorro de CO₂ al mover entrenamiento entre regiones y priorizar horas verdes.

Visual principal: Gráfico waterfall
Soporte: Heatmap hora vs región, Bar chart ranking, Línea temporal
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import (
    bundle_has_carbon,
    bundle_has_usage,
    data_source_caption,
    load_gold_bundle,
    page1_extras,
)
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 1: Movilidad regional y horas verdes."""
    
    st.markdown('<p class="page-header">📊 Página 1: Movilidad Regional y Horas Verdes</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Ahorro de CO₂ al mover entrenamiento entre regiones y priorizar horas verdes</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    df_usage = bundle["usage"]
    df_carbon = bundle["carbon"]
    df_region = bundle["region"]
    extras = page1_extras(bundle)
    from_gold = bundle_has_usage(bundle) or bundle_has_carbon(bundle)
    if df_usage.empty and df_carbon.empty:
        st.info("📌 Usando datos de demostración")
        df_usage = get_demo_data()
        df_carbon = get_demo_carbon_data()
        df_region = get_demo_region_data()
        from_gold = False
    data_source_caption(from_gold, "Heatmap y KPIs desde `fact_carbon_intensity_hourly` / `fact_ai_compute_usage`.")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    total_savings = extras.get("total_emissions_tons", 1250.0)
    rz = extras.get("ranking_zones")
    avg_reduction = 35
    best_dest = "eu-north-1"
    if rz is not None and not rz.empty and "Reducción %" in rz.columns:
        avg_reduction = float(rz["Reducción %"].mean())
        best_dest = str(rz.iloc[0]["Región Destino"])
    
    with col1:
        st.metric("🌱 Emisiones acumuladas (tCO₂eq)", f"{float(total_savings):,.0f}")
    
    with col2:
        st.metric("📉 Reducción relativa media (zonas)", f"{avg_reduction:.0f}%")
    
    with col3:
        st.metric("🌍 Zona más limpia (intensidad)", best_dest)
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Waterfall ====================
    st.subheader("💧 Gráfico Waterfall: Emisiones Origen → Destino → CO₂ Evitado")
    
    # Datos para waterfall
    waterfall_data = pd.DataFrame({
        'Concepto': ['Emisiones Origen (us-east-1)', 'Emisiones Destino (eu-north-1)', 'CO₂ Evitado'],
        'Valor': [4500, 2800, -1700],
        'Tipo': ['total', 'total', 'difference']
    })
    
    fig_waterfall = go.Figure(go.Waterfall(
        name="20", orientation="v",
        measure=["relative", "relative", "total"],
        x=["Origen: us-east-1", "Destino: eu-north-1", "CO₂ Evitado"],
        textposition="outside",
        text=[f"{v:,.0f}" for v in [4500, 2800, 1700]],
        y=[4500, -1700, 1700],
        connector={"line":{"color":"rgb(63, 63, 63)"}},
        decreasing={"marker":{"color":"#10b981"}},
        increasing={"marker":{"color":"#ef4444"}},
        totals={"marker":{"color":"#3b82f6"}}
    ))
    
    fig_waterfall.update_layout(
        title="Ahorro de CO₂ al Migrar Cargas de us-east-1 a eu-north-1",
        showlegend=False,
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2")
    )
    
    fig_waterfall = apply_control_tower_plotly_theme(fig_waterfall)
    
    st.plotly_chart(fig_waterfall, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_heatmap, col_ranking = st.columns(2)
    
    with col_heatmap:
        st.subheader("🗓️ Heatmap: Hora vs Región con Intensidad de Carbono")
        
        # Heatmap de intensidad por hora y región
        heatmap_data = df_carbon.groupby(['zone', 'hour']).agg({
            'carbon_intensity_gco2eq_per_kwh': 'mean'
        }).reset_index()
        
        if not heatmap_data.empty:
            heatmap_pivot = heatmap_data.pivot(
                index='zone', 
                columns='hour', 
                values='carbon_intensity_gco2eq_per_kwh'
            ).fillna(0)
            
            fig_heatmap = px.imshow(
                heatmap_pivot,
                labels=dict(x="Hora del Día", y="Zona", color="gCO₂eq/kWh"),
                color_continuous_scale="Greens",
                aspect="auto"
            )
            fig_heatmap.update_layout(
                height=350,
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="#f2f2f2")
            )
            fig_heatmap = apply_control_tower_plotly_theme(fig_heatmap)
            st.plotly_chart(fig_heatmap, width='stretch')
        else:
            # Demo heatmap
            demo_heatmap = create_demo_heatmap()
            demo_heatmap = apply_control_tower_plotly_theme(demo_heatmap)
            st.plotly_chart(demo_heatmap, width='stretch')
    
    with col_ranking:
        st.subheader("🏆 Ranking: Destinos con Mayor Reducción %")
        
        ranking_data = extras.get("ranking_zones")
        if ranking_data is None or ranking_data.empty:
            ranking_data = pd.DataFrame({
                'Región Destino': ['eu-north-1', 'eu-central-1', 'ca-central-1', 'us-west-2', 'ap-northeast-1'],
                'Reducción %': [38, 32, 28, 22, 18],
                'Intensidad Carbono': [45, 85, 120, 180, 290]
            })
        
        fig_ranking = px.bar(
            ranking_data,
            y='Región Destino',
            x='Reducción %',
            orientation='h',
            color='Reducción %',
            color_continuous_scale='Greens',
            title="Ranking de Destinos por % de Reducción"
        )
        fig_ranking.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#f2f2f2")
        )
        fig_ranking = apply_control_tower_plotly_theme(fig_ranking)
        st.plotly_chart(fig_ranking, width='stretch')
    
    # ==================== LÍNEA TEMPORAL ====================
    st.markdown("---")
    st.subheader("📈 Línea Temporal: Intensidad de Carbono Origen vs Destino")
    
    tl = extras.get("timeline")
    if tl is not None and not tl.empty and len(tl.columns) > 2:
        timeline_data = tl
        mes_col = "Mes" if "Mes" in timeline_data.columns else timeline_data.columns[0]
        fig_timeline = go.Figure()
        for col in timeline_data.columns:
            if col == mes_col:
                continue
            fig_timeline.add_trace(go.Scatter(
                x=timeline_data[mes_col],
                y=timeline_data[col],
                mode='lines+markers',
                name=str(col),
                line=dict(width=2),
            ))
    else:
        timeline_data = pd.DataFrame({
            'Mes': ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
            'us-east-1': [420, 415, 430, 410, 395, 380, 375, 380, 390, 405, 420, 425],
            'eu-north-1': [280, 275, 290, 270, 255, 240, 235, 245, 260, 275, 285, 290]
        })
        fig_timeline = go.Figure()
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['Mes'],
            y=timeline_data['us-east-1'],
            mode='lines+markers',
            name='us-east-1 (Origen)',
            line=dict(color='#ef4444', width=2)
        ))
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['Mes'],
            y=timeline_data['eu-north-1'],
            mode='lines+markers',
            name='eu-north-1 (Destino)',
            line=dict(color='#10b981', width=2)
        ))
    
    fig_timeline.update_layout(
        title="Intensidad de Carbono Mensual: Origen vs Destino",
        xaxis_title="Mes",
        yaxis_title="gCO₂eq/kWh",
        height=350,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2"),
        legend=dict(orientation="h", y=1.1)
    )
    
    fig_timeline = apply_control_tower_plotly_theme(fig_timeline)
    
    st.plotly_chart(fig_timeline, width='stretch')
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **waterfall** muestra claramente el antes vs después y el ahorro neto
    - El **heatmap** permite identificar visualmente las horas verdes
    - El **ranking** facilita la decisión de destino
    - La **línea temporal** muestra estacionalidad y tendencias
    """)


def get_demo_data():
    """Datos de demostración para uso de cómputo."""
    return pd.DataFrame({
        'session_id': [f'session_{i}' for i in range(200)],
        'job_type': ['Training'] * 80 + ['Inference'] * 70 + ['Fine-tuning'] * 50,
        'execution_status': ['Success'] * 160 + ['Failed'] * 40,
        'region': ['us-east-1'] * 60 + ['us-west-2'] * 50 + ['eu-west-1'] * 50 + ['eu-north-1'] * 40,
        'gpu_model': ['A100'] * 70 + ['H100'] * 60 + ['V100'] * 40 + ['T4'] * 30,
        'energy_consumed_kwh': [100 + i * 5 for i in range(200)],
        'cost_electricity_usd': [50 + i * 2.5 for i in range(200)],
        'duration_hours': [1 + i * 0.05 for i in range(200)]
    })


def get_demo_carbon_data():
    """Datos de demostración para intensidad de carbono."""
    zones = ['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'AR', 'CL', 'CO', 'DE', 'FR', 'SE', 'GB']
    data = []
    for zone in zones:
        for hour in range(24):
            base_intensity = 50 + (hash(zone) % 200)
            # Simular variación horaria
            hour_factor = 1 + 0.3 * np.sin(2 * np.pi * hour / 24)
            data.append({
                'zone': zone,
                'hour': hour,
                'carbon_intensity_gco2eq_per_kwh': base_intensity * hour_factor
            })
    return pd.DataFrame(data)


def get_demo_region_data():
    """Datos de demostración para regiones."""
    return pd.DataFrame({
        'region_id': ['us-east-1', 'us-west-2', 'eu-west-1', 'eu-north-1'],
        'aws_region_code': ['us-east-1', 'us-west-2', 'eu-west-1', 'eu-north-1'],
        'aws_region_name': ['US East (N. Virginia)', 'US West (Oregon)', 'EU (Ireland)', 'EU (Stockholm)'],
        'electricity_maps_zone': ['US-CA', 'US-TX', 'GB', 'SE'],
        'country_name': ['Estados Unidos', 'Estados Unidos', 'Reino Unido', 'Suecia']
    })


def create_demo_heatmap():
    """Crear heatmap de demostración."""
    zones = ['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'DE', 'FR', 'SE']
    hours = list(range(24))
    
    data = []
    for zone in zones:
        base = 50 + (hash(zone) % 150)
        for hour in hours:
            # Simular patrón: más bajo en horas nocturnas
            hour_factor = 1 + 0.4 * np.sin(2 * np.pi * (hour - 6) / 24)
            data.append({
                'zone': zone,
                'hour': hour,
                'intensity': base * hour_factor
            })
    
    df = pd.DataFrame(data)
    pivot = df.pivot(index='zone', columns='hour', values='intensity')
    
    fig = px.imshow(
        pivot,
        labels=dict(x="Hora del Día", y="Zona", color="gCO₂eq/kWh"),
        color_continuous_scale="Greens",
        aspect="auto"
    )
    fig.update_layout(
        height=350,
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2")
    )
    return fig


if __name__ == "__main__":
    render()