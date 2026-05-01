"""
Página 9: Comparación por tipo de carga y resultado
====================================================
Pregunta: diferencias entre Training, Fine-tuning e Inference, y entre Success vs Failed.

Visual principal: Bar chart agrupado (job_type × execution_status)
Soporte: 100% stacked bar, Boxplot, Tabla resumen
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

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
    """Renderizar Página 9: Comparación por tipo de carga y resultado."""
    
    st.markdown('<p class="page-header">🔄 Página 9: Tipo de Carga vs Resultado</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Diferencias entre Training, Fine-tuning e Inference, y entre Success vs Failed</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("⚡ Energía Desperdiciada", "125 MWh")
    
    with col2:
        st.metric("💻 Job Más Costoso", "Training")
    
    with col3:
        st.metric("📊 Tasa de Fallas", "18%")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Bar Chart Agrupado ====================
    st.subheader("📊 Bar Chart: Energía por Tipo de Job y Estado")
    
    # Datos de jobs
    job_data = pd.DataFrame({
        'job_type': ['Training'] * 2 + ['Fine-tuning'] * 2 + ['Inference'] * 2,
        'execution_status': ['Success', 'Failed'] * 3,
        'energy_kwh': [45000, 12000, 28000, 8500, 15000, 3200],
        'cost_usd': [22500, 6000, 14000, 4250, 7500, 1600],
        'emissions_kg': [15750, 4200, 9800, 2975, 5250, 1120]
    })
    
    # Selector de métrica
    metric = st.radio(
        "Seleccionar Métrica",
        options=['energy_kwh', 'cost_usd', 'emissions_kg'],
        format_func=lambda x: {'energy_kwh': 'Energía (kWh)', 'cost_usd': 'Costo ($)', 'emissions_kg': 'Emisiones (kgCO₂)'}[x],
        horizontal=True
    )
    
    fig_bar = px.bar(
        job_data,
        x='job_type',
        y=metric,
        color='execution_status',
        barmode='group',
        title="Métricas por Tipo de Job y Estado de Ejecución",
        color_discrete_map={
            'Success': '#10b981',
            'Failed': '#ef4444'
        }
    )
    
    fig_bar.update_layout(
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff"),
        legend=dict(orientation="h", y=1.05)
    )
    
    fig_bar = aplicar_tema_plotly(fig_bar)
    
    st.plotly_chart(fig_bar, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_stack, col_box = st.columns(2)
    
    with col_stack:
        st.subheader("📈 100% Stacked Bar: Proporción de Failed")
        
        # Calcular proporciones
        stacked_data = pd.DataFrame({
            'job_type': ['Training', 'Fine-tuning', 'Inference'],
            'Success': [79, 77, 82],
            'Failed': [21, 23, 18]
        })
        
        fig_stack = px.bar(
            stacked_data,
            x='job_type',
            y=['Success', 'Failed'],
            title="Proporción Success vs Failed por Tipo de Job",
            color_discrete_map={
                'Success': '#10b981',
                'Failed': '#ef4444'
            },
            labels={'value': 'Porcentaje', 'job_type': 'Tipo de Job'}
        )
        
        fig_stack.update_layout(
            height=350,
            barmode='stack',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff"),
            yaxis_title="Porcentaje"
        )
        fig_stack = aplicar_tema_plotly(fig_stack)
        st.plotly_chart(fig_stack, width='stretch')
    
    with col_box:
        st.subheader("📦 Boxplot: Distribución de Energía por Tipo de Carga")
        
        # Crear datos para boxplot
        boxplot_data = []
        for job_type in ['Training', 'Fine-tuning', 'Inference']:
            for status in ['Success', 'Failed']:
                base = 500 if job_type == 'Training' else 300 if job_type == 'Fine-tuning' else 100
                for _ in range(50):
                    boxplot_data.append({
                        'job_type': job_type,
                        'execution_status': status,
                        'energy_kwh': base + np.random.exponential(100)
                    })
        
        df_box = pd.DataFrame(boxplot_data)
        
        fig_box = px.box(
            df_box,
            x='job_type',
            y='energy_kwh',
            color='execution_status',
            title="Distribución de Energía Consumida",
            color_discrete_map={
                'Success': '#10b981',
                'Failed': '#ef4444'
            }
        )
        
        fig_box.update_layout(
            height=350,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_box = aplicar_tema_plotly(fig_box)
        st.plotly_chart(fig_box, width='stretch')
    
    # ==================== TABLA RESUMEN ====================
    st.markdown("---")
    st.subheader("📋 Tabla Resumen: Costo Evitable por Failed Runs")
    
    summary_data = pd.DataFrame({
        'Tipo de Job': ['Training', 'Fine-tuning', 'Inference', 'Total'],
        'Sesiones Success': [450, 280, 520, 1250],
        'Sesiones Failed': [120, 85, 110, 315],
        'Tasa de Fallas': ['21%', '23%', '18%', '20%'],
        'Energía Success (MWh)': [45, 28, 15, 88],
        'Energía Failed (MWh)': [12, 8.5, 3.2, 23.7],
        'Costo Evitable ($)': [6000, 4250, 1600, 11850]
    })
    
    st.dataframe(
        summary_data.style.apply(
            lambda x: ['background: #10b981' if 'Total' in str(x.name) else '' for i in x], 
            axis=1
        ),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **bar chart agrupado** muestra claramente las dos dimensiones categóricas
    - El **100% stacked bar** visualiza la proporción de failed
    - El **boxplot** revela la distribución de energía por tipo de carga
    - La **tabla resumen** cuantifica el costo evitable
    """)


if __name__ == "__main__":
    render()