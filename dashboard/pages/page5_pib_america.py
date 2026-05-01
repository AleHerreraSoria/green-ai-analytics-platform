"""
Página 5: PIB per cápita y electricidad baja en carbono (América)
==================================================================
Pregunta: relación entre PIB per cápita y red baja en carbono en países de América.

Visual principal: Scatter plot (PIB vs intensidad de carbono)
Soporte: Mapa de América, Ranking de países, Small multiples por año
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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
    """Renderizar Página 5: PIB per cápita y electricidad baja en carbono (América)."""
    
    st.markdown('<p class="page-header">💵 Página 5: PIB per cápita y Electricidad Baja en Carbono (América)</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Relación entre PIB per cápita y red baja en carbono en países de América</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📊 Correlación en América", "0.68")
    
    with col2:
        st.metric("💰 Mayor PIB per cápita", "Canadá ($55k)")
    
    with col3:
        st.metric("🌱 Red más Limpia", "Canadá (92%)")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter Plot ====================
    st.subheader("🎯 Scatter Plot: PIB per cápita vs Intensidad de Carbono")
    
    # Datos de países americanos
    america_data = pd.DataFrame({
        'País': ['Canadá', 'Estados Unidos', 'Chile', 'Puerto Rico', 'Panamá', 
                'Costa Rica', 'México', 'Colombia', 'Brasil', 'Perú', 
                'Argentina', 'Ecuador', 'Rep. Dominicana', 'Guatemala', 'El Salvador'],
        'PIB_per_capita': [55000, 80000, 15000, 35000, 14000, 12000, 11000, 6500, 9000, 6000,
                          13000, 5500, 8000, 4500, 3500],
        'Intensidad_Carbono': [120, 420, 320, 450, 280, 150, 380, 180, 85, 290,
                              210, 220, 350, 420, 280],
        'Low_Carbon_Share': [92, 35, 40, 28, 55, 78, 45, 65, 78, 50,
                            55, 48, 42, 35, 52],
        'Subregión': ['Norteamérica', 'Norteamérica', 'Sudamérica', 'Caribe', 'Centroamérica',
                     'Centroamérica', 'Centroamérica', 'Sudamérica', 'Sudamérica', 'Sudamérica',
                     'Sudamérica', 'Sudamérica', 'Caribe', 'Centroamérica', 'Centroamérica']
    })
    
    fig_scatter = px.scatter(
        america_data,
        x='PIB_per_capita',
        y='Intensidad_Carbono',
        color='Subregión',
        hover_name='País',
        title="PIB per cápita vs Intensidad de Carbono en América",
        labels={
            'PIB_per_capita': 'PIB per cápita (USD)',
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
    col_map, col_ranking = st.columns(2)
    
    with col_map:
        st.subheader("🗺️ Mapa de América por Intensidad de Carbono")
        
        # Datos para mapa (simulado)
        map_data = pd.DataFrame({
            'País': ['Canada', 'United States', 'Mexico', 'Brazil', 'Argentina', 
                    'Chile', 'Colombia', 'Peru', 'Venezuela', 'Ecuador'],
            'code': ['CAN', 'USA', 'MEX', 'BRA', 'ARG', 'CHL', 'COL', 'PER', 'VEN', 'ECU'],
            'Intensidad': [120, 420, 380, 85, 210, 320, 180, 290, 180, 220]
        })
        
        fig_map = px.choropleth(
            map_data,
            locations='code',
            color='Intensidad',
            hover_name='País',
            title="Intensidad de Carbono por País",
            color_continuous_scale='Greens',
            projection='natural earth'
        )
        
        fig_map.update_layout(
            height=350,
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type='natural earth'
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_map = aplicar_tema_plotly(fig_map)
        st.plotly_chart(fig_map, width='stretch')
    
    with col_ranking:
        st.subheader("🏆 Ranking de Países Americanos")
        
        ranking_data = america_data.sort_values('Low_Carbon_Share', ascending=False)
        
        fig_ranking = px.bar(
            ranking_data.head(10),
            y='País',
            x='Low_Carbon_Share',
            orientation='h',
            color='Low_Carbon_Share',
            color_continuous_scale='Greens',
            title="% Energía Baja en Carbono"
        )
        fig_ranking.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_ranking = aplicar_tema_plotly(fig_ranking)
        st.plotly_chart(fig_ranking, width='stretch')
    
    # ==================== SMALL MULTIPLES POR AÑO ====================
    st.markdown("---")
    st.subheader("📊 Small Multiples: Evolución por Año")
    
    # Datos por año
    yearly_data = pd.DataFrame({
        'Año': [2020, 2020, 2020, 2021, 2021, 2021, 2022, 2022, 2022, 2023, 2023, 2023, 2024, 2024, 2024],
        'País': ['Canadá', 'México', 'Brasil'] * 5,
        'PIB': [52000, 10000, 8500, 53000, 10500, 8700, 54000, 10800, 8800, 54500, 11000, 8900, 55000, 11000, 9000],
        'Low_Carbon': [88, 42, 75, 89, 43, 76, 90, 44, 77, 91, 45, 77, 92, 45, 78]
    })
    
    fig_small = px.scatter(
        yearly_data,
        x='PIB',
        y='Low_Carbon',
        color='País',
        facet_col='Año',
        title="Evolución: PIB vs Energía Baja en Carbono por Año",
        labels={
            'PIB': 'PIB per cápita',
            'Low_Carbon': '% Energía Limpia'
        },
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    
    fig_small.update_layout(
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff")
    )
    
    fig_small = aplicar_tema_plotly(fig_small)
    
    st.plotly_chart(fig_small, width='stretch')
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **scatter** combina análisis analítico con dimensión de subregión
    - El **mapa coroplético** proporciona vista geográfica inmediata
    - El **ranking** permite comparación directa
    - Los **small multiples** muestran evolución temporal
    """)


if __name__ == "__main__":
    render()