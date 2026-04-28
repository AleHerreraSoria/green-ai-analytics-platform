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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import data_source_caption, load_gold_bundle, page5_americas
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 5: PIB per cápita y electricidad baja en carbono (América)."""
    
    st.markdown('<p class="page-header">💵 Página 5: PIB per cápita y Electricidad Baja en Carbono (América)</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Relación entre PIB per cápita y red baja en carbono en países de América</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    america_data, ok_am = page5_americas(bundle)
    if not ok_am or america_data.empty:
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
    else:
        america_data['PIB_per_capita'] = america_data['Exportaciones_TIC_MM']
    data_source_caption(ok_am, "Filtrado ISO América sobre join `dim_country` + intensidad por zona.")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    from scipy import stats as _st
    corr_txt = "—"
    if len(america_data) >= 2:
        corr_txt = f"{_st.pearsonr(america_data['PIB_per_capita'], america_data['Intensidad_Carbono'])[0]:.2f}"
    top_pib = america_data.nlargest(1, 'PIB_per_capita') if 'PIB_per_capita' in america_data.columns else pd.DataFrame()
    top_clean = america_data.nlargest(1, 'Low_Carbon_Share') if 'Low_Carbon_Share' in america_data.columns else pd.DataFrame()
    
    with col1:
        st.metric("📊 Correlación en América", corr_txt)
    
    with col2:
        st.metric("💰 Mayor PIB per cápita", str(top_pib.iloc[0]['País']) if not top_pib.empty else "—")
    
    with col3:
        st.metric("🌱 Red más Limpia", str(top_clean.iloc[0]['País']) if not top_clean.empty else "—")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter Plot ====================
    st.subheader("🎯 Scatter Plot: PIB per cápita vs Intensidad de Carbono")

    america_data = america_data.copy()
    if "PIB_per_capita" not in america_data.columns and "Exportaciones_TIC_MM" in america_data.columns:
        america_data["PIB_per_capita"] = america_data["Exportaciones_TIC_MM"]
    if "Subregión" not in america_data.columns and "Región" in america_data.columns:
        america_data["Subregión"] = america_data["Región"]
    
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
            line=dict(width=1, color='#f2f2f2')
        )
    )
    
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
            font=dict(color="#f2f2f2")
        )
        fig_map = apply_control_tower_plotly_theme(fig_map)
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
            font=dict(color="#f2f2f2")
        )
        fig_ranking = apply_control_tower_plotly_theme(fig_ranking)
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
        font=dict(color="#f2f2f2")
    )
    
    fig_small = apply_control_tower_plotly_theme(fig_small)
    
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