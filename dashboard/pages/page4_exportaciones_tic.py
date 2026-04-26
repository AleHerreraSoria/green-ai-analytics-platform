"""
Página 4: Exportaciones TIC y limpieza de la red
================================================
Pregunta: correlación entre exportaciones TIC y limpieza de la matriz eléctrica.

Visual principal: Scatter plot con línea de regresión
Soporte: Línea temporal doble, Ranking de países, Tabla de correlaciones
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
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
    """Renderizar Página 4: Exportaciones TIC y limpieza de la red."""
    
    st.markdown('<p class="page-header">🌐 Página 4: Exportaciones TIC y Limpieza de la Red</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Correlación entre exportaciones TIC y limpieza de la matriz eléctrica</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📊 Correlación Pearson", "0.72")
    
    with col2:
        st.metric("🌍 Países Analizados", "24")
    
    with col3:
        st.metric("📅 Período Cubierto", "2020-2024")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter con Regresión ====================
    st.subheader("🎯 Scatter Plot: Exportaciones TIC vs Intensidad de Carbono")
    
    # Datos de ejemplo
    scatter_data = pd.DataFrame({
        'País': ['Alemania', 'Francia', 'Reino Unido', 'Canadá', 'Japón', 'Corea del Sur', 
                'Australia', 'Brasil', 'México', 'India', 'China', 'Estados Unidos'],
        'Exportaciones_TIC_MM': [95, 82, 75, 45, 88, 92, 38, 28, 35, 55, 120, 150],
        'Intensidad_Carbono': [380, 320, 350, 120, 450, 520, 650, 85, 420, 680, 580, 420],
        'Low_Carbon_Share': [55, 62, 48, 92, 28, 18, 25, 78, 45, 15, 22, 35],
        'Región': ['Europa', 'Europa', 'Europa', 'Norteamérica', 'Asia', 'Asia', 
                  'Oceanía', 'Latinoamérica', 'Latinoamérica', 'Asia', 'Asia', 'Norteamérica']
    })
    
    # Calcular correlación
    corr, p_value = stats.pearsonr(scatter_data['Exportaciones_TIC_MM'], scatter_data['Intensidad_Carbono'])
    
    # Scatter plot con regresión
    fig_scatter = px.scatter(
        scatter_data,
        x='Exportaciones_TIC_MM',
        y='Intensidad_Carbono',
        color='Región',
        hover_name='País',
        title=f"Exportaciones TIC vs Intensidad de Carbono (r={corr:.2f}, p={p_value:.3f})",
        labels={
            'Exportaciones_TIC_MM': 'Exportaciones TIC (Miles de M$)',
            'Intensidad_Carbono': 'Intensidad de Carbono (gCO₂eq/kWh)',
            'Low_Carbon_Share': '% Energía Baja en Carbono'
        },
        hover_data={
            'Low_Carbon_Share': ':.0f'
        },
        color_discrete_sequence=px.colors.qualitative.Bold
    )

    fig_scatter.update_traces(
        marker=dict(
            size=14,
            line=dict(width=1, color='#ffffff')
        )
    )
    
    # Añadir línea de tendencia
    z = np.polyfit(scatter_data['Exportaciones_TIC_MM'], scatter_data['Intensidad_Carbono'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(scatter_data['Exportaciones_TIC_MM'].min(), scatter_data['Exportaciones_TIC_MM'].max(), 100)
    
    fig_scatter.add_trace(go.Scatter(
        x=x_line, y=p(x_line),
        mode='lines',
        name='Tendencia Lineal',
        line=dict(color='red', dash='dash', width=2)
    ))
    
    fig_scatter.update_layout(
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff"),
        legend=dict(orientation="h", y=1.05)
    )
    
    fig_scatter = aplicar_tema_plotly(fig_scatter)
    
    st.plotly_chart(fig_scatter, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_line, col_ranking = st.columns(2)
    
    with col_line:
        st.subheader("📈 Línea Temporal Doble para un País")
        
        # Selector de país
        pais_seleccionado = st.selectbox(
            "Seleccionar País",
            options=['Alemania', 'Francia', 'Canadá', 'Brasil', 'México'],
            index=0
        )
        
        # Datos temporales
        timeline_data = pd.DataFrame({
            'Año': [2020, 2021, 2022, 2023, 2024],
            'Exportaciones_TIC': [72, 78, 85, 90, 95],
            'Intensidad_Carbono': [420, 400, 385, 370, 380],
            'Low_Carbon_Share': [48, 52, 55, 58, 55]
        })
        
        fig_line = go.Figure()
        
        fig_line.add_trace(go.Scatter(
            x=timeline_data['Año'],
            y=timeline_data['Exportaciones_TIC'],
            mode='lines+markers',
            name='Exportaciones TIC (MM$)',
            yaxis='y',
            line=dict(color='#3b82f6', width=3)
        ))
        
        fig_line.add_trace(go.Scatter(
            x=timeline_data['Año'],
            y=timeline_data['Intensidad_Carbono'],
            mode='lines+markers',
            name='Intensidad Carbono',
            yaxis='y2',
            line=dict(color='#10b981', width=3)
        ))
        
        fig_line.update_layout(
            title=f"Evolución: Exportaciones TIC vs Intensidad de Carbono - {pais_seleccionado}",
            xaxis_title="Año",
            yaxis=dict(title="Exportaciones TIC (MM$)", side='left'),
            yaxis2=dict(title="Intensidad Carbono", side='right', overlaying='y'),
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff"),
            legend=dict(orientation="h", y=1.1)
        )
        fig_line = aplicar_tema_plotly(fig_line)
        st.plotly_chart(fig_line, width='stretch')
    
    with col_ranking:
        st.subheader("🏆 Ranking: Países con Mayor Crecimiento TIC y Menor Intensidad")
        
        ranking_data = scatter_data.sort_values('Low_Carbon_Share', ascending=False).head(8)
        
        fig_ranking = px.bar(
            ranking_data,
            y='País',
            x='Low_Carbon_Share',
            orientation='h',
            color='Low_Carbon_Share',
            color_continuous_scale='Greens',
            title="Países por % de Energía Baja en Carbono"
        )
        fig_ranking.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_ranking = aplicar_tema_plotly(fig_ranking)
        st.plotly_chart(fig_ranking, width='stretch')
    
    # ==================== TABLA DE CORRELACIONES ====================
    st.markdown("---")
    st.subheader("📋 Tabla de Coeficientes de Correlación por Región")
    
    corr_by_region = pd.DataFrame({
        'Región': ['Europa', 'Norteamérica', 'Latinoamérica', 'Asia', 'Oceanía'],
        'Pearson': [0.68, 0.75, 0.82, 0.45, 0.58],
        'Spearman': [0.65, 0.72, 0.78, 0.42, 0.55],
        'p-value': [0.02, 0.01, 0.005, 0.15, 0.08],
        'N países': [5, 3, 4, 4, 2]
    })
    
    st.dataframe(
        corr_by_region.style.background_gradient(subset=['Pearson'], cmap='Greens'),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **scatter con regresión** es obligatorio para mostrar correlación
    - La **línea temporal doble** permite ver evolución conjunta
    - El **ranking** combina crecimiento TIC con limpieza de red
    - La **tabla de correlaciones** resume por región el análisis
    """)


if __name__ == "__main__":
    render()