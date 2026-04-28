"""
Página 7: Ventanas operativas de cómputo verde
===============================================
Pregunta: franjas horarias donde conviene concentrar o aplazar cargas.

Visual principal: Heatmap de 24 horas × zona
Soporte: Línea horaria, Boxplot, Tabla de mejores franjas
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import bundle_has_carbon, data_source_caption, load_gold_bundle
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 7: Ventanas operativas de cómputo verde."""
    
    st.markdown('<p class="page-header">⏰ Página 7: Ventanas Operativas de Cómputo Verde</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Franjas horarias donde conviene concentrar o aplazar cargas</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    cf = bundle["carbon"]
    ok_c = bundle_has_carbon(bundle)
    if ok_c and {"zone", "hour", "carbon_intensity_gco2eq_per_kwh"}.issubset(cf.columns):
        df_heatmap = cf.groupby(["zone", "hour"], as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
        df_heatmap = df_heatmap.rename(columns={"zone": "Zona", "hour": "Hora", "carbon_intensity_gco2eq_per_kwh": "Intensidad"})
    else:
        zones = ['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'DE', 'FR', 'SE', 'GB', 'JP']
        heatmap_data = []
        for zone in zones:
            base = 50 + (hash(zone) % 200)
            for hour in range(24):
                if 6 <= hour <= 9:
                    hour_factor = 1.3 + (hour - 6) * 0.1
                elif 18 <= hour <= 21:
                    hour_factor = 1.4 + (hour - 18) * 0.05
                elif 0 <= hour <= 5:
                    hour_factor = 0.6
                else:
                    hour_factor = 1.0
                heatmap_data.append({'Zona': zone, 'Hora': hour, 'Intensidad': base * hour_factor})
        df_heatmap = pd.DataFrame(heatmap_data)
    data_source_caption(ok_c, "`fact_carbon_intensity_hourly` agregado por zona y hora.")

    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    hour_avg = df_heatmap.groupby("Hora")["Intensidad"].mean() if not df_heatmap.empty else None
    best_hours = "—"
    if hour_avg is not None and len(hour_avg):
        hmin = hour_avg.idxmin()
        best_hours = f"{int(hmin):02d}:00"
    glob_mean = float(df_heatmap["Intensidad"].mean()) if not df_heatmap.empty else 85.0
    below = (df_heatmap["Intensidad"] < 150).mean() * 100.0 if not df_heatmap.empty else 42.0

    with col1:
        st.metric("🌱 Hora Más Limpia (promedio zonas)", best_hours)
    
    with col2:
        st.metric("📊 Intensidad media (grid)", f"{glob_mean:.0f} gCO₂/kWh")
    
    with col3:
        st.metric("📈 % registros bajo 150 g/kWh", f"{below:.0f}%")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Heatmap ====================
    st.subheader("🗺️ Heatmap: 24 Horas × Zona con Intensidad de Carbono")
    heatmap_pivot = df_heatmap.pivot(index='Zona', columns='Hora', values='Intensidad')
    
    fig_heatmap = px.imshow(
        heatmap_pivot,
        labels=dict(x="Hora del Día", y="Zona", color="gCO₂eq/kWh"),
        color_continuous_scale='Greens',
        aspect='auto'
    )
    
    fig_heatmap.update_layout(
        title="Intensidad de Carbono por Hora y Zona",
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#f2f2f2"),
        xaxis_title="Hora del Día",
        yaxis_title="Zona"
    )
    
    fig_heatmap = apply_control_tower_plotly_theme(fig_heatmap)
    
    st.plotly_chart(fig_heatmap, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_line, col_box = st.columns(2)
    
    with col_line:
        st.subheader("📈 Línea Horaria para Zona Seleccionada")
        
        zonas_opts = sorted(df_heatmap['Zona'].unique().tolist()) if not df_heatmap.empty else ['US-CA', 'US-TX']
        zona_seleccionada = st.selectbox(
            "Seleccionar Zona",
            options=zonas_opts,
            index=0
        )
        
        # Datos de línea
        line_data = df_heatmap[df_heatmap['Zona'] == zona_seleccionada].sort_values('Hora')
        
        fig_line = px.line(
            line_data,
            x='Hora',
            y='Intensidad',
            title=f"Intensidad de Carbono por Hora - {zona_seleccionada}",
            markers=True
        )
        
        # Añadir línea de umbral
        fig_line.add_hline(
            y=100, 
            line_dash="dash", 
            line_color="green",
            annotation_text="Umbral Verde"
        )
        
        fig_line.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#f2f2f2"),
            xaxis_title="Hora del Día",
            yaxis_title="gCO₂eq/kWh"
        )
        fig_line = apply_control_tower_plotly_theme(fig_line)
        st.plotly_chart(fig_line, width='stretch')
    
    with col_box:
        st.subheader("📦 Boxplot por Hora del Día")
        
        # Crear datos para boxplot (simular distribución)
        boxplot_data = []
        for hour in range(24):
            base = 100 + (hour % 12) * 20
            for _ in range(50):
                boxplot_data.append({
                    'Hora': hour,
                    'Intensidad': base + np.random.normal(0, 20)
                })
        
        df_box = pd.DataFrame(boxplot_data)
        
        fig_box = px.box(
            df_box,
            x='Hora',
            y='Intensidad',
            title="Distribución de Intensidad por Hora"
        )
        
        fig_box.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#f2f2f2")
        )
        fig_box = apply_control_tower_plotly_theme(fig_box)
        st.plotly_chart(fig_box, width='stretch')
    
    # ==================== TABLA DE MEJORES FRANJAS ====================
    st.markdown("---")
    st.subheader("📋 Tabla de Mejores Franjas Horarias")
    
    # Calcular mejores franjas
    best_hours = pd.DataFrame({
        'Franja Horaria': ['00:00 - 04:00', '04:00 - 06:00', '06:00 - 08:00', 
                          '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00'],
        'Intensidad Promedio': [65, 72, 95, 110, 105, 125],
        'Recomendación': ['✅ Ideal', '✅ Muy Bueno', '⚠️ Moderado', 
                         '⚠️ Moderado', '❌ Alto', '❌ Muy Alto'],
        'Ahorro vs Hora Pico': ['48%', '42%', '24%', '12%', '16%', '0%']
    })
    
    st.dataframe(
        best_hours.style.apply(
            lambda x: ['background: #10b981' if 'Ideal' in str(x) else 
                     'background: #22c55e' if 'Muy Bueno' in str(x) else
                     'background: #f59e0b' if 'Moderado' in str(x) else
                     'background: #ef4444' for i in x], 
            axis=1
        ),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **heatmap** es ideal para ver patrones hora × zona
    - La **línea horaria** permite análisis detallado por zona
    - El **boxplot** muestra dispersión y valores atípicos
    - La **tabla de mejores franjas** sintetiza recomendaciones actionables
    """)


if __name__ == "__main__":
    render()