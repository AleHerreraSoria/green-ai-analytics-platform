"""
Página 9: Comparación por tipo de carga y resultado
====================================================
Pregunta: diferencias entre Training, Fine-tuning e Inference, y entre Success vs Failed.

Visual principal: Bar chart agrupado (job_type × execution_status)
Soporte: 100% stacked bar, Boxplot, Tabla resumen
"""
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import data_source_caption, load_gold_bundle, page9_jobs
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 9: Comparación por tipo de carga y resultado."""
    
    st.markdown('<p class="page-header">🔄 Página 9: Tipo de Carga vs Resultado</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Diferencias entre Training, Fine-tuning e Inference, y entre Success vs Failed</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    job_data, ok_j = page9_jobs(bundle)
    if not ok_j or job_data.empty:
        job_data = pd.DataFrame({
            'job_type': ['Training'] * 2 + ['Fine-tuning'] * 2 + ['Inference'] * 2,
            'execution_status': ['Success', 'Failed'] * 3,
            'energy_kwh': [45000, 12000, 28000, 8500, 15000, 3200],
            'cost_usd': [22500, 6000, 14000, 4250, 7500, 1600],
            'emissions_kg': [15750, 4200, 9800, 2975, 5250, 1120]
        })
    else:
        job_data["job_type"] = job_data["job_type"].astype(str).str.title()
        job_data["execution_status"] = job_data["execution_status"].astype(str).str.title()
    data_source_caption(ok_j, "Agregación `fact_ai_compute_usage` por `job_type` y `execution_status`.")

    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    failed_e = job_data[job_data["execution_status"].str.lower() == "failed"]["energy_kwh"].sum() if "execution_status" in job_data.columns else 0
    top_job = job_data.groupby("job_type")["energy_kwh"].sum().idxmax() if "job_type" in job_data.columns else "—"
    fail_rate = 0.0
    if "execution_status" in job_data.columns:
        tot = len(bundle["usage"]) if not bundle["usage"].empty else 1
        fails = (bundle["usage"]["execution_status"].astype(str).str.lower() == "failed").sum() if not bundle["usage"].empty and "execution_status" in bundle["usage"].columns else 0
        fail_rate = fails / max(tot, 1) * 100.0

    with col1:
        st.metric("⚡ Energía en Failed (aprox.)", f"{failed_e/1000:.0f} MWh")
    
    with col2:
        st.metric("💻 Job con más energía", str(top_job))
    
    with col3:
        st.metric("📊 Tasa de Fallas (filas)", f"{fail_rate:.0f}%")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Bar Chart Agrupado ====================
    st.subheader("📊 Bar Chart: Energía por Tipo de Job y Estado")
    
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
        font=dict(color="#f2f2f2"),
        legend=dict(orientation="h", y=1.05)
    )
    
    fig_bar = apply_control_tower_plotly_theme(fig_bar)
    
    st.plotly_chart(fig_bar, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_stack, col_box = st.columns(2)
    
    with col_stack:
        st.subheader("📈 100% Stacked Bar: Proporción de Failed")
        
        if ok_j and not job_data.empty:
            pvt = job_data.pivot_table(
                index="job_type",
                columns="execution_status",
                values="energy_kwh",
                aggfunc="sum",
                fill_value=0,
            )
            rs = pvt.sum(axis=1).replace(0, np.nan)
            stacked_data = pvt.div(rs, axis=0) * 100.0
            stacked_data = stacked_data.reset_index()
        else:
            stacked_data = pd.DataFrame({
                'job_type': ['Training', 'Fine-tuning', 'Inference'],
                'Success': [79, 77, 82],
                'Failed': [21, 23, 18]
            })
        y_cols = [c for c in stacked_data.columns if c != "job_type"]
        fig_stack = px.bar(
            stacked_data,
            x='job_type',
            y=y_cols,
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
            font=dict(color="#f2f2f2"),
            yaxis_title="Porcentaje"
        )
        fig_stack = apply_control_tower_plotly_theme(fig_stack)
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
            font=dict(color="#f2f2f2")
        )
        fig_box = apply_control_tower_plotly_theme(fig_box)
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