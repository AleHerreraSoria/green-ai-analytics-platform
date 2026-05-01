"""
Página 6: Escenario +20% adopción de IA en Latam
================================================
Pregunta: incremento proyectado de demanda y emisiones por país ante +20% de adopción.

Visual principal: Bar chart apilado (base + incremento + total)
Soporte: Mapa coroplético, Waterfall regional, Tabla escenario
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import data_source_caption, load_gold_bundle, page6_latam_scenario
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 6: Escenario +20% adopción de IA en Latam."""
    
    st.markdown('<p class="page-header">📈 Página 6: Escenario +20% Adopción de IA en Latam</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Incremento proyectado de demanda y emisiones por país ante +20% de adopción</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    scenario_data, ok_sc = page6_latam_scenario(bundle)
    if not ok_sc or scenario_data.empty:
        scenario_data = pd.DataFrame({
            'País': ['Brasil', 'México', 'Argentina', 'Colombia', 'Chile', 'Perú'],
            'Base_kWh': [4500000, 3200000, 1800000, 1200000, 950000, 850000],
            'Incremento_kWh': [900000, 640000, 360000, 240000, 190000, 170000],
            'Intensidad_Carbono': [85, 380, 210, 180, 320, 290]
        })
    data_source_caption(ok_sc, "Base = suma `energy_consumed_kwh` por país (ISO Latam); +20% escenario.")

    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    inc_kwh = scenario_data['Incremento_kWh'].sum() if 'Incremento_kWh' in scenario_data.columns else 0
    scenario_data = scenario_data.copy()
    scenario_data['Delta_tCO2'] = scenario_data['Incremento_kWh'] * scenario_data['Intensidad_Carbono'] / 1e6
    inc_co2 = scenario_data['Delta_tCO2'].sum()
    worst = scenario_data.nlargest(1, 'Delta_tCO2') if 'Delta_tCO2' in scenario_data.columns else pd.DataFrame()

    with col1:
        st.metric("⚡ Aumento Total kWh", f"+{inc_kwh/1e6:.2f}M kWh")
    
    with col2:
        st.metric("🌍 Aumento Total Emisiones", f"+{inc_co2:,.0f} tCO₂")
    
    with col3:
        st.metric("🔴 País Mayor Impacto", str(worst.iloc[0]['País']) if not worst.empty else "—")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Bar Chart Apilado ====================
    st.subheader("📊 Bar Chart: Base + Incremento + Total por País")
    
    scenario_data['Total_kWh'] = scenario_data['Base_kWh'] + scenario_data['Incremento_kWh']
    scenario_data['Emisiones_Base'] = scenario_data['Base_kWh'] * scenario_data['Intensidad_Carbono'] / 1000000
    scenario_data['Emisiones_Incremento'] = scenario_data['Incremento_kWh'] * scenario_data['Intensidad_Carbono'] / 1000000
    
    # Bar chart agrupado
    fig_bar = go.Figure()
    
    fig_bar.add_trace(go.Bar(
        x=scenario_data['País'],
        y=scenario_data['Base_kWh'],
        name='Base Actual',
        marker_color='#3b82f6'
    ))
    
    fig_bar.add_trace(go.Bar(
        x=scenario_data['País'],
        y=scenario_data['Incremento_kWh'],
        name='Incremento (+20%)',
        marker_color='#10b981'
    ))
    
    fig_bar.update_layout(
        title="Demanda de Energía: Base vs Escenario +20%",
        xaxis_title="País",
        yaxis_title="kWh",
        barmode='stack',
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2"),
        legend=dict(orientation="h", y=1.1)
    )
    
    fig_bar = apply_control_tower_plotly_theme(fig_bar)
    
    st.plotly_chart(fig_bar, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_map, col_waterfall = st.columns(2)
    
    with col_map:
        st.subheader("🗺️ Mapa Coroplético: Delta de Emisiones")
        
        map_data = pd.DataFrame({
            'País': ['Brazil', 'Mexico', 'Argentina', 'Colombia', 'Chile', 'Peru'],
            'code': ['BRA', 'MEX', 'ARG', 'COL', 'CHL', 'PER'],
            'Delta_Emisiones': [765, 486, 151, 86, 121, 98]
        })
        
        fig_map = px.choropleth(
            map_data,
            locations='code',
            color='Delta_Emisiones',
            hover_name='País',
            title="Incremento de Emisiones (tCO₂eq)",
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
    
    with col_waterfall:
        st.subheader("💧 Waterfall: Total Regional")
        
        waterfall_data = pd.DataFrame({
            'Concepto': ['Base Actual', 'Incremento', 'Total Escenario'],
            'Valor': [12500000, 2500000, 15000000]
        })
        
        fig_waterfall = go.Figure(go.Waterfall(
            name="Escenario",
            orientation="v",
            measure=["relative", "relative", "total"],
            x=["Base", "Incremento (+20%)", "Total"],
            textposition="outside",
            text=["12.5M", "2.5M", "15.0M"],
            y=[12500000, 2500000, 15000000],
            connector={"line":{"color":"rgb(63, 63, 63)"}},
            decreasing={"marker":{"color":"#10b981"}},
            increasing={"marker":{"color":"#3b82f6"}},
            totals={"marker":{"color":"#f59e0b"}}
        ))
        
        fig_waterfall.update_layout(
            title="Waterfall: Demanda Total Regional",
            showlegend=False,
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#f2f2f2")
        )
        fig_waterfall = apply_control_tower_plotly_theme(fig_waterfall)
        st.plotly_chart(fig_waterfall, width='stretch')
    
    # ==================== TABLA DE ESCENARIO ====================
    st.markdown("---")
    st.subheader("📋 Tabla de Escenario por País")
    
    table_data = pd.DataFrame({
        'País': ['Brasil', 'México', 'Argentina', 'Colombia', 'Chile', 'Perú', 'Total'],
        'kWh Base': ['4.5M', '3.2M', '1.8M', '1.2M', '950K', '850K', '12.5M'],
        'kWh Nuevo': ['5.4M', '3.8M', '2.2M', '1.4M', '1.1M', '1.0M', '15.0M'],
        'Delta kWh': ['+900K', '+640K', '+360K', '+240K', '+190K', '+170K', '+2.5M'],
        'Delta Emisiones': ['+765', '+486', '+151', '+86', '+121', '+98', '+1,707'],
        '% sobre Demanda País': ['20%', '20%', '20%', '20%', '20%', '20%', '20%']
    })
    
    st.dataframe(
        table_data.style.apply(lambda x: ['background: #10b981' if 'Total' in str(x.name) else '' for i in x], axis=1),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **bar chart apilado** muestra base + incremento + total claramente
    - El **mapa coroplético** visualiza el impacto geográfico
    - El **waterfall** resume el total regional
    - La **tabla de escenario** proporciona valores exactos para planificación
    """)


if __name__ == "__main__":
    render()