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
from gold_analytics import data_source_caption, load_gold_bundle, page8_blindspots
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 8: Puntos ciegos de sostenibilidad."""
    
    st.markdown('<p class="page-header">⚠️ Página 8: Puntos Ciegos de Sostenibilidad</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Territorios con alta demanda tecnológica y red muy intensiva en carbono</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    territory_data, ok_b = page8_blindspots(bundle)
    if not ok_b or territory_data.empty:
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
    data_source_caption(ok_b, "Demanda = energía agregada por región AWS (proxy); intensidad = media grid por zona.")

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

    col_k1, col_k2, col_k3 = st.columns(3)
    n_crit = int((territory_data["Cuadrante"] == "Crítico").sum())
    worst = territory_data.nlargest(1, "Score_Riesgo") if "Score_Riesgo" in territory_data.columns else pd.DataFrame()
    avg_r = float(territory_data["Score_Riesgo"].mean()) if "Score_Riesgo" in territory_data.columns else 0.0
    with col_k1:
        st.metric("🔴 Territorios Críticos", str(n_crit))
    with col_k2:
        st.metric("⚠️ Mayor Score", str(worst.iloc[0]["Territorio"]) if not worst.empty else "—")
    with col_k3:
        st.metric("📊 Score Promedio", f"{avg_r:.0f}")

    st.markdown("---")
    
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
            line=dict(width=1, color='#f2f2f2')
        )
    )
    
    # Añadir líneas de corte
    fig_scatter.add_vline(x=50, line_dash="dash", line_color="white", opacity=0.5)
    fig_scatter.add_hline(y=400, line_dash="dash", line_color="white", opacity=0.5)
    
    fig_scatter.update_layout(
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2"),
        legend=dict(orientation="h", y=1.05)
    )
    
    fig_scatter = apply_control_tower_plotly_theme(fig_scatter)
    
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
            font=dict(color="#f2f2f2")
        )
        fig_matrix = apply_control_tower_plotly_theme(fig_matrix)
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
            font=dict(color="#f2f2f2")
        )
        fig_ranking = apply_control_tower_plotly_theme(fig_ranking)
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
        font=dict(color="#f2f2f2")
    )
    fig_map = apply_control_tower_plotly_theme(fig_map)
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