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
from s3_connection import load_fact_table, load_dimension


def aplicar_tema_plotly(fig):
    """Aplicar estilo común a gráficos Plotly sobre fondo verde oscuro.

    Objetivos:
    - textos blancos en títulos, ejes, leyendas y anotaciones;
    - leyendas horizontales debajo del área del gráfico para no superponer títulos;
    - ocultar barras laterales de escala/color;
    - mantener fondo transparente para integrarse con Streamlit.
    """
    text_color = "#ffffff"
    grid_color = "rgba(255,255,255,0.25)"
    transparent = "rgba(0,0,0,0)"

    fig.update_layout(
        font=dict(color=text_color),
        title=dict(
            font=dict(color=text_color, size=15),
            x=0,
            xanchor="left",
            y=0.98,
            yanchor="top"
        ),
        plot_bgcolor=transparent,
        paper_bgcolor=transparent,
        margin=dict(l=70, r=30, t=85, b=105),
        coloraxis_showscale=False,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="center",
            x=0.5,
            font=dict(color=text_color, size=11),
            title_font=dict(color=text_color, size=11),
            bgcolor=transparent,
            borderwidth=0,
            itemsizing="constant",
            tracegroupgap=6
        )
    )

    # Ocultar cualquier escala de color asociada a coloraxis, coloraxis2, etc.
    for layout_key in list(fig.layout):
        if str(layout_key).startswith("coloraxis"):
            try:
                fig.layout[layout_key].showscale = False
            except Exception:
                pass

    fig.update_xaxes(
        title_font=dict(color=text_color),
        tickfont=dict(color=text_color),
        color=text_color,
        gridcolor=grid_color,
        zerolinecolor=grid_color,
        linecolor=grid_color
    )

    fig.update_yaxes(
        title_font=dict(color=text_color),
        tickfont=dict(color=text_color),
        color=text_color,
        gridcolor=grid_color,
        zerolinecolor=grid_color,
        linecolor=grid_color
    )

    fig.update_annotations(font=dict(color=text_color))

    try:
        fig.update_geos(
            bgcolor=transparent,
            lakecolor=transparent,
            landcolor="rgba(0,56,23,0.20)",
            coastlinecolor=grid_color,
            framecolor=grid_color
        )
    except Exception:
        pass

    # Ocultar barras laterales de escala/color en trazas individuales.
    for trace in fig.data:
        try:
            trace.update(showscale=False)
        except Exception:
            pass

        if hasattr(trace, "marker") and trace.marker is not None:
            try:
                trace.marker.update(showscale=False)
            except Exception:
                pass

        if hasattr(trace, "textfont"):
            try:
                trace.update(textfont=dict(color=text_color))
            except Exception:
                pass

    return fig

def render():
    """Renderizar Página 1: Movilidad regional y horas verdes."""
    
    st.markdown('<p class="page-header">📊 Página 1: Movilidad Regional y Horas Verdes</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Ahorro de CO₂ al mover entrenamiento entre regiones y priorizar horas verdes</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Cargar datos
    try:
        df_usage = load_fact_table("fact_ai_compute_usage")
        df_carbon = load_fact_table("fact_carbon_intensity_hourly")
        df_region = load_dimension("dim_region")
        
        if df_usage.empty:
            raise ValueError("Sin datos")
    except:
        st.info("📌 Usando datos de demostración")
        df_usage = get_demo_data()
        df_carbon = get_demo_carbon_data()
        df_region = get_demo_region_data()
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    # Calcular métricas
    total_savings = 1250  # tCO₂eq evitadas (demo)
    avg_reduction = 35  # %
    best_origin, best_dest = "us-east-1", "eu-north-1"
    
    with col1:
        st.metric("🌱 tCO₂eq Evitadas", f"{total_savings:,.0f}")
    
    with col2:
        st.metric("📉 % de Reducción", f"{avg_reduction}%")
    
    with col3:
        st.metric("🌍 Mejor Destino", best_dest)
    
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
        font=dict(color="#ffffff")
    )
    
    fig_waterfall = aplicar_tema_plotly(fig_waterfall)
    
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
                font=dict(color="#ffffff")
            )
            fig_heatmap = aplicar_tema_plotly(fig_heatmap)
            st.plotly_chart(fig_heatmap, width='stretch')
        else:
            # Demo heatmap
            demo_heatmap = create_demo_heatmap()
            demo_heatmap = aplicar_tema_plotly(demo_heatmap)
            st.plotly_chart(demo_heatmap, width='stretch')
    
    with col_ranking:
        st.subheader("🏆 Ranking: Destinos con Mayor Reducción %")
        
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
            font=dict(color="#ffffff")
        )
        fig_ranking = aplicar_tema_plotly(fig_ranking)
        st.plotly_chart(fig_ranking, width='stretch')
    
    # ==================== LÍNEA TEMPORAL ====================
    st.markdown("---")
    st.subheader("📈 Línea Temporal: Intensidad de Carbono Origen vs Destino")
    
    # Datos de serie temporal
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
        font=dict(color="#ffffff"),
        legend=dict(orientation="h", y=1.1)
    )
    
    fig_timeline = aplicar_tema_plotly(fig_timeline)
    
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
        font=dict(color="#ffffff")
    )
    return fig


if __name__ == "__main__":
    render()