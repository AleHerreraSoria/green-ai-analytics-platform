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
    """Renderizar Página 7: Ventanas operativas de cómputo verde."""
    
    st.markdown('<p class="page-header">⏰ Página 7: Ventanas Operativas de Cómputo Verde</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Franjas horarias donde conviene concentrar o aplazar cargas</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🌱 Hora Más Limpia", "03:00 - 05:00")
    
    with col2:
        st.metric("📊 Promedio Horas Verdes", "85 gCO₂/kWh")
    
    with col3:
        st.metric("📈 % Horas bajo Umbral", "42%")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Heatmap ====================
    st.subheader("🗺️ Heatmap: 24 Horas × Zona con Intensidad de Carbono")
    
    # Generar datos de heatmap
    zones = ['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'DE', 'FR', 'SE', 'GB', 'JP']
    hours = list(range(24))
    
    heatmap_data = []
    for zone in zones:
        base = 50 + (hash(zone) % 200)
        for hour in hours:
            # Simular patrón: más bajo en horas nocturnas, pico en horas pico
            if 6 <= hour <= 9:
                hour_factor = 1.3 + (hour - 6) * 0.1
            elif 18 <= hour <= 21:
                hour_factor = 1.4 + (hour - 18) * 0.05
            elif 0 <= hour <= 5:
                hour_factor = 0.6
            else:
                hour_factor = 1.0
            
            heatmap_data.append({
                'Zona': zone,
                'Hora': hour,
                'Intensidad': base * hour_factor
            })
    
    df_heatmap = pd.DataFrame(heatmap_data)
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
        font=dict(color="#ffffff"),
        xaxis_title="Hora del Día",
        yaxis_title="Zona"
    )
    
    fig_heatmap = aplicar_tema_plotly(fig_heatmap)
    
    st.plotly_chart(fig_heatmap, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_line, col_box = st.columns(2)
    
    with col_line:
        st.subheader("📈 Línea Horaria para Zona Seleccionada")
        
        zona_seleccionada = st.selectbox(
            "Seleccionar Zona",
            options=['US-CA', 'US-TX', 'US-NY', 'BR', 'MX', 'DE', 'FR', 'SE'],
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
            font=dict(color="#ffffff"),
            xaxis_title="Hora del Día",
            yaxis_title="gCO₂eq/kWh"
        )
        fig_line = aplicar_tema_plotly(fig_line)
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
            font=dict(color="#ffffff")
        )
        fig_box = aplicar_tema_plotly(fig_box)
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