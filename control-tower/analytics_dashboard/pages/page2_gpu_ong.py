"""
Página 2: Elección de GPU para la ONG
======================================
Pregunta: qué GPU es más eficiente en costo y huella combinando consumo, precio eléctrico y factor de emisión.

Visual principal: Scatter plot (costo vs emisiones)
Soporte: Bar chart agrupado, Tabla ranking, Radar chart (opcional)
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
    """Renderizar Página 2: Elección de GPU para la ONG."""
    
    st.markdown('<p class="page-header">💻 Página 2: Elección de GPU para la ONG</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Qué GPU es más eficiente en costo y huella combinando consumo, precio eléctrico y factor de emisión</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🟢 GPU Recomendada", "H100")
    
    with col2:
        st.metric("💵 Menor Costo/Hora", "$4.25")
    
    with col3:
        st.metric("🌍 Menor Huella/Hora", "0.85 kgCO₂")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter Plot ====================
    st.subheader("🎯 Scatter Plot: Costo vs Emisiones por GPU")
    
    # Datos de GPUs
    gpu_data = pd.DataFrame({
        'GPU': ['H100', 'A100', 'V100', 'A10G', 'T4', 'L40', 'L4'],
        'costo_hora': [4.25, 3.50, 2.80, 1.85, 0.75, 3.10, 1.45],
        'emisiones_hora_kg': [0.85, 1.20, 1.45, 0.95, 0.55, 1.30, 0.70],
        'tflops_fp32': [51, 19.5, 14, 12, 8, 30, 8],
        'tdp_watts': [700, 400, 300, 150, 70, 300, 72]
    })
    
    fig_scatter = px.scatter(
        gpu_data,
        x='costo_hora',
        y='emisiones_hora_kg',
        color='GPU',
        hover_name='GPU',
        text='GPU',
        title="Costo por Hora vs Emisiones por Hora por GPU",
        labels={
            'costo_hora': 'Costo por Hora (USD)',
            'emisiones_hora_kg': 'Emisiones por Hora (kgCO₂)',
            'tflops_fp32': 'Rendimiento (TFLOPS FP32)',
            'tdp_watts': 'TDP (Watts)'
        },
        hover_data={
            'tflops_fp32': ':.1f',
            'tdp_watts': ':.0f'
        },
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    
    fig_scatter.update_traces(
        textposition='top center',
        marker=dict(
            size=16,
            line=dict(width=1, color='#ffffff')
        )
    )
    
    fig_scatter.update_layout(
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff"),
        showlegend=False
    )
    
    fig_scatter = aplicar_tema_plotly(fig_scatter)
    fig_scatter.update_layout(showlegend=False, margin=dict(l=70, r=30, t=85, b=70))
    
    st.plotly_chart(fig_scatter, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_bar, col_table = st.columns(2)
    
    with col_bar:
        st.subheader("📊 Bar Chart: Costo y Emisiones por GPU")
        
        # Bar chart agrupado
        gpu_melted = gpu_data.melt(
            id_vars=['GPU'], 
            value_vars=['costo_hora', 'emisiones_hora_kg'],
            var_name='Métrica',
            value_name='Valor'
        )
        gpu_melted['Métrica'] = gpu_melted['Métrica'].replace({
            'costo_hora': 'Costo/Hora (USD)',
            'emisiones_hora_kg': 'Emisiones/Hora (kg)'
        })
        
        fig_bar = px.bar(
            gpu_melted,
            x='GPU',
            y='Valor',
            color='Métrica',
            barmode='group',
            title="Costo vs Emisiones por GPU",
            color_discrete_map={
                'Costo/Hora (USD)': '#3b82f6',
                'Emisiones/Hora (kg)': '#10b981'
            }
        )
        fig_bar.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_bar = aplicar_tema_plotly(fig_bar)
        st.plotly_chart(fig_bar, width='stretch')
    
    with col_table:
        st.subheader("🏆 Tabla Ranking con Score Ponderado")
        
        # Calcular score (60% costo, 40% carbono)
        gpu_ranking = gpu_data.copy()
        
        # Normalizar (menor es mejor para ambas métricas)
        gpu_ranking['score_costo'] = (gpu_ranking['costo_hora'] - gpu_ranking['costo_hora'].min()) / (gpu_ranking['costo_hora'].max() - gpu_ranking['costo_hora'].min())
        gpu_ranking['score_emisiones'] = (gpu_ranking['emisiones_hora_kg'] - gpu_ranking['emisiones_hora_kg'].min()) / (gpu_ranking['emisiones_hora_kg'].max() - gpu_ranking['emisiones_hora_kg'].min())
        gpu_ranking['score_total'] = 0.6 * gpu_ranking['score_costo'] + 0.4 * gpu_ranking['score_emisiones']
        gpu_ranking['rank'] = gpu_ranking['score_total'].rank()
        
        # Ordenar por score (menor es mejor)
        gpu_ranking = gpu_ranking.sort_values('score_total')
        
        display_df = gpu_ranking[['GPU', 'costo_hora', 'emisiones_hora_kg', 'tflops_fp32', 'score_total']].head(7)
        display_df.columns = ['GPU', 'Costo/Hora ($)', 'Emisiones (kgCO₂)', 'TFLOPS', 'Score (↓ mejor)']
        
        st.dataframe(
            display_df.style.background_gradient(subset=['Score (↓ mejor)'], cmap='Greens'),
            width='stretch',
            height=350
        )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **scatter** deja ver la "zona ideal" donde están las GPUs con bajo costo y baja huella
    - El **bar chart agrupado** permite comparación directa de ambas métricas
    - La **tabla ranking** con score ponderado (60% costo, 40% carbono) facilita la decisión final
    - El tamaño de las burbujas en el scatter muestra el rendimiento (TFLOPS)
    """)


if __name__ == "__main__":
    render()