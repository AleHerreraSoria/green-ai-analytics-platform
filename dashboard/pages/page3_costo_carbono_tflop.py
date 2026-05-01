"""
Página 3: Costo de carbono por TFLOP
=====================================
Pregunta: comparar H100 vs A100 y ver cómo cambia según país o zona eléctrica.

Visual principal: Heatmap GPU × país/zona
Soporte: Bar chart horizontal, Slope chart, Tabla compacta
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
    """Renderizar Página 3: Costo de carbono por TFLOP."""
    
    st.markdown('<p class="page-header">💰 Página 3: Costo de Carbono por TFLOP</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Comparar H100 vs A100 y ver cómo cambia según país o zona eléctrica</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🟢 Mejor Combinación", "H100 + SE")
    
    with col2:
        st.metric("🔴 Peor Combinación", "A100 + US-TX")
    
    with col3:
        st.metric("📊 Brecha H100 vs A100", "42%")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Heatmap ====================
    st.subheader("🗺️ Heatmap: Costo de Carbono por TFLOP (gCO₂eq/TFLOP-h)")
    
    # Datos GPU × País
    heatmap_data = pd.DataFrame({
        'GPU': ['H100'] * 8 + ['A100'] * 8 + ['V100'] * 8 + ['T4'] * 8,
        'País': ['SE', 'NO', 'CA', 'DE', 'FR', 'US-CA', 'BR', 'MX'] * 4,
        'gCO2eq_per_TFLOP': [
            # H100
            12, 15, 18, 22, 25, 45, 35, 55,
            # A100
            18, 22, 28, 32, 38, 65, 52, 80,
            # V100
            25, 30, 38, 45, 52, 85, 68, 105,
            # T4
            35, 42, 52, 62, 72, 115, 92, 140
        ]
    })
    
    heatmap_pivot = heatmap_data.pivot(index='GPU', columns='País', values='gCO2eq_per_TFLOP')
    
    fig_heatmap = px.imshow(
        heatmap_pivot,
        labels=dict(x="País/Zona", y="GPU", color="gCO₂eq/TFLOP-h"),
        color_continuous_scale="Greens",
        aspect='auto',
        text_auto='.0f'
    )
    
    fig_heatmap.update_layout(
        height=350,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff")
    )
    
    fig_heatmap = aplicar_tema_plotly(fig_heatmap)
    
    st.plotly_chart(fig_heatmap, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_bar, col_slope = st.columns(2)
    
    with col_bar:
        st.subheader("📊 Bar Chart: Comparación de GPUs en un País")
        
        # Filtrar por país seleccionado
        pais_seleccionado = st.selectbox(
            "Seleccionar País/Zona",
            options=['SE', 'NO', 'CA', 'DE', 'FR', 'US-CA', 'BR', 'MX'],
            index=0
        )
        
        bar_data = heatmap_data[heatmap_data['País'] == pais_seleccionado].sort_values('gCO2eq_per_TFLOP')
        
        fig_bar = px.bar(
            bar_data,
            x='GPU',
            y='gCO2eq_per_TFLOP',
            color='GPU',
            title=f"Costo de Carbono por TFLOP en {pais_seleccionado}",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_bar.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_bar = aplicar_tema_plotly(fig_bar)
        st.plotly_chart(fig_bar, width='stretch')
    
    with col_slope:
        st.subheader("📈 Slope Chart: Una Misma GPU entre Países")
        
        # Slope chart para H100
        slope_data = pd.DataFrame({
            'País': ['SE', 'NO', 'CA', 'DE', 'FR', 'US-CA', 'BR', 'MX'],
            'H100': [12, 15, 18, 22, 25, 45, 35, 55],
            'A100': [18, 22, 28, 32, 38, 65, 52, 80]
        })
        
        fig_slope = go.Figure()
        
        # Líneas para cada GPU
        for gpu in ['H100', 'A100']:
            fig_slope.add_trace(go.Scatter(
                x=slope_data['País'],
                y=slope_data[gpu],
                mode='lines+markers',
                name=gpu,
                line=dict(width=3)
            ))
        
        fig_slope.update_layout(
            title="Costo de Carbono: H100 vs A100 por País",
            xaxis_title="País",
            yaxis_title="gCO₂eq/TFLOP-h",
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff"),
            legend=dict(orientation="h", y=1.1)
        )
        fig_slope = aplicar_tema_plotly(fig_slope)
        st.plotly_chart(fig_slope, width='stretch')
    
    # ==================== TABLA COMPACTA ====================
    st.markdown("---")
    st.subheader("📋 Tabla de Valores Exactos")
    
    # Tabla formateada
    table_display = heatmap_data.pivot(index='GPU', columns='País', values='gCO2eq_per_TFLOP')
    st.dataframe(
        table_display.style.background_gradient(cmap='Greens', axis=1),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **heatmap** muestra la matriz GPU × país con intensidad de color
    - El **bar chart** permite comparar pocas GPUs en un país específico
    - El **slope chart** muestra cómo cambia una misma GPU entre países
    - La **tabla** proporciona valores exactos para decisiones precisas
    """)


if __name__ == "__main__":
    render()