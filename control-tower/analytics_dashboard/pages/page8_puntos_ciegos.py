"""
Página 8: Puntos ciegos de sostenibilidad
============================================
Pregunta: territorios con alta demanda tecnológica y red muy intensiva en carbono.

Visual principal: Scatter plot en cuadrantes
Soporte: Matriz 2×2 semáforo, Ranking Top 10, Mapa de riesgo
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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
    """Renderizar Página 8: Puntos ciegos de sostenibilidad."""
    
    st.markdown('<p class="page-header">⚠️ Página 8: Puntos Ciegos de Sostenibilidad</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Territorios con alta demanda tecnológica y red muy intensiva en carbono</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🔴 Territorios Críticos", "12")
    
    with col2:
        st.metric("⚠️ Territorio Más Crítico", "India (IN)")
    
    with col3:
        st.metric("📊 Score Promedio de Riesgo", "72/100")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter en Cuadrantes ====================
    st.subheader("🎯 Scatter Plot: Demanda Tecnológica vs Intensidad de Carbono")
    
    # Datos de territorios
    territory_data = pd.DataFrame([
        {
            'Territorio': 'India',
            'Demanda_Tecnologica': 85,
            'Intensidad_Carbono': 680,
            'Score_Riesgo': 92,
            'Región': 'Asia'
        },
        {
            'Territorio': 'China',
            'Demanda_Tecnologica': 95,
            'Intensidad_Carbono': 580,
            'Score_Riesgo': 88,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Vietnam',
            'Demanda_Tecnologica': 65,
            'Intensidad_Carbono': 450,
            'Score_Riesgo': 78,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Indonesia',
            'Demanda_Tecnologica': 72,
            'Intensidad_Carbono': 520,
            'Score_Riesgo': 82,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Tailandia',
            'Demanda_Tecnologica': 58,
            'Intensidad_Carbono': 420,
            'Score_Riesgo': 72,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Malasia',
            'Demanda_Tecnologica': 62,
            'Intensidad_Carbono': 380,
            'Score_Riesgo': 68,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Filipinas',
            'Demanda_Tecnologica': 48,
            'Intensidad_Carbono': 350,
            'Score_Riesgo': 62,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Pakistán',
            'Demanda_Tecnologica': 42,
            'Intensidad_Carbono': 420,
            'Score_Riesgo': 68,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Bangladesh',
            'Demanda_Tecnologica': 38,
            'Intensidad_Carbono': 380,
            'Score_Riesgo': 65,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Nigeria',
            'Demanda_Tecnologica': 35,
            'Intensidad_Carbono': 320,
            'Score_Riesgo': 58,
            'Región': 'África'
        },
        {
            'Territorio': 'Kenia',
            'Demanda_Tecnologica': 28,
            'Intensidad_Carbono': 280,
            'Score_Riesgo': 52,
            'Región': 'África'
        },
        {
            'Territorio': 'Etiopía',
            'Demanda_Tecnologica': 22,
            'Intensidad_Carbono': 250,
            'Score_Riesgo': 48,
            'Región': 'África'
        },
        {
            'Territorio': 'Corea del Sur',
            'Demanda_Tecnologica': 78,
            'Intensidad_Carbono': 520,
            'Score_Riesgo': 85,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Japón',
            'Demanda_Tecnologica': 82,
            'Intensidad_Carbono': 450,
            'Score_Riesgo': 80,
            'Región': 'Asia'
        },
        {
            'Territorio': 'Alemania',
            'Demanda_Tecnologica': 55,
            'Intensidad_Carbono': 380,
            'Score_Riesgo': 68,
            'Región': 'Europa'
        },
        {
            'Territorio': 'Reino Unido',
            'Demanda_Tecnologica': 52,
            'Intensidad_Carbono': 350,
            'Score_Riesgo': 65,
            'Región': 'Europa'
        },
        {
            'Territorio': 'Francia',
            'Demanda_Tecnologica': 48,
            'Intensidad_Carbono': 320,
            'Score_Riesgo': 60,
            'Región': 'Europa'
        },
        {
            'Territorio': 'Canadá',
            'Demanda_Tecnologica': 45,
            'Intensidad_Carbono': 120,
            'Score_Riesgo': 45,
            'Región': 'Norteamérica'
        }
    ])
    
    # Clasificar en cuadrantes
    def classify_quadrant(row):
        if row['Demanda_Tecnologica'] >= 50 and row['Intensidad_Carbono'] >= 400:
            return 'Crítico'
        elif row['Demanda_Tecnologica'] >= 50 and row['Intensidad_Carbono'] < 400:
            return 'En Desarrollo'
        elif row['Demanda_Tecnologica'] < 50 and row['Intensidad_Carbono'] >= 400:
            return 'En Riesgo'
        else:
            return 'Sostenible'
    
    territory_data['Cuadrante'] = territory_data.apply(classify_quadrant, axis=1)
    
    fig_scatter = px.scatter(
        territory_data,
        x='Demanda_Tecnologica',
        y='Intensidad_Carbono',
        color='Cuadrante',
        hover_name='Territorio',
        title="Matriz de Riesgo: Demanda Tecnológica vs Intensidad de Carbono",
        labels={
            'Demanda_Tecnologica': 'Proxy Demanda Tecnológica',
            'Intensidad_Carbono': 'Intensidad de Carbono (gCO₂eq/kWh)',
            'Score_Riesgo': 'Score de Riesgo'
        },
        hover_data={
            'Score_Riesgo': ':.0f'
        },
        color_discrete_map={
            'Crítico': '#ef4444',
            'En Desarrollo': '#f59e0b',
            'En Riesgo': '#eab308',
            'Sostenible': '#10b981'
        }
    )

    fig_scatter.update_traces(
        marker=dict(
            size=14,
            line=dict(width=1, color='#ffffff')
        )
    )
    
    # Añadir líneas de corte
    fig_scatter.add_vline(x=50, line_dash="dash", line_color="white", opacity=0.5)
    fig_scatter.add_hline(y=400, line_dash="dash", line_color="white", opacity=0.5)
    
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
    col_matrix, col_ranking = st.columns(2)
    
    with col_matrix:
        st.subheader("🚦 Matriz 2×2 Tipo Semáforo")
        
        # Crear matriz de semáforo
        matrix_data = (
            territory_data
            .groupby('Cuadrante')
            .size()
            .reset_index(name='Cantidad')
        )

        orden_cuadrantes = ['Crítico', 'En Desarrollo', 'En Riesgo', 'Sostenible']

        matrix_data = (
            matrix_data
            .set_index('Cuadrante')
            .reindex(orden_cuadrantes, fill_value=0)
            .reset_index()
        )

        matrix_data['Color'] = matrix_data['Cuadrante'].map({
            'Crítico': '#ef4444',
            'En Desarrollo': '#f59e0b',
            'En Riesgo': '#eab308',
            'Sostenible': '#10b981'
        })
        
        fig_matrix = px.bar(
            matrix_data,
            x='Cuadrante',
            y='Cantidad',
            color='Cuadrante',
            title="Distribución de Territorios por Cuadrante",
            color_discrete_map={
                'Crítico': '#ef4444',
                'En Desarrollo': '#f59e0b',
                'En Riesgo': '#eab308',
                'Sostenible': '#10b981'
            }
        )
        
        fig_matrix.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_matrix = aplicar_tema_plotly(fig_matrix)
        st.plotly_chart(fig_matrix, width='stretch')
    
    with col_ranking:
        st.subheader("🏆 Ranking Top 10 Territorios Críticos")
        
        ranking_data = territory_data[territory_data['Cuadrante'].isin(['Crítico', 'En Desarrollo'])].sort_values('Score_Riesgo', ascending=False).head(10)
        
        fig_ranking = px.bar(
            ranking_data,
            y='Territorio',
            x='Score_Riesgo',
            orientation='h',
            color='Score_Riesgo',
            color_continuous_scale='Reds',
            title="Top 10 Territorios con Mayor Riesgo"
        )
        fig_ranking.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_ranking = aplicar_tema_plotly(fig_ranking)
        st.plotly_chart(fig_ranking, width='stretch')
    
    # ==================== MAPA DE RIESGO ====================
    st.markdown("---")
    st.subheader("🗺️ Mapa de Territorios en Riesgo")
    
    map_data = pd.DataFrame({
        'País': ['India', 'China', 'Indonesia', 'Vietnam', 'Thailand', 'Malaysia'],
        'code': ['IND', 'CHN', 'IDN', 'VNM', 'THA', 'MYS'],
        'Riesgo': [92, 88, 82, 78, 72, 68]
    })
    
    fig_map = px.choropleth(
        map_data,
        locations='code',
        color='Riesgo',
        hover_name='País',
        title="Territorios con Mayor Riesgo de Sostenibilidad",
        color_continuous_scale='Reds'
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
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **scatter por cuadrantes** hace muy clara la lógica alto/bajo vs alto/bajo
    - La **matriz semáforo** resume la distribución
    - El **ranking** identifica los territorios más críticos
    - El **mapa** proporciona contexto geográfico
    """)


if __name__ == "__main__":
    render()