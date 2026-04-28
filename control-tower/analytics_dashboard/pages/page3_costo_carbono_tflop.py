"""
Página 3: Costo de carbono por TFLOP
=====================================
Pregunta: comparar H100 vs A100 y ver cómo cambia según país o zona eléctrica.

Visual principal: Heatmap GPU × país/zona
Soporte: Bar chart horizontal, Slope chart, Tabla compacta
"""
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import data_source_caption, load_gold_bundle, page3_tflop_matrix
from plotly_theme import apply_control_tower_plotly_theme



def render():
    """Renderizar Página 3: Costo de carbono por TFLOP."""
    
    st.markdown('<p class="page-header">💰 Página 3: Costo de Carbono por TFLOP</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Comparar H100 vs A100 y ver cómo cambia según país o zona eléctrica</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    bundle = load_gold_bundle()
    heatmap_data, ok_g = page3_tflop_matrix(bundle)
    if not ok_g or heatmap_data.empty:
        heatmap_data = pd.DataFrame({
            'GPU': ['H100'] * 8 + ['A100'] * 8 + ['V100'] * 8 + ['T4'] * 8,
            'País': ['SE', 'NO', 'CA', 'DE', 'FR', 'US-CA', 'BR', 'MX'] * 4,
            'gCO2eq_per_TFLOP': [
                12, 15, 18, 22, 25, 45, 35, 55,
                18, 22, 28, 32, 38, 65, 52, 80,
                25, 30, 38, 45, 52, 85, 68, 105,
                35, 42, 52, 62, 72, 115, 92, 140
            ]
        })
    data_source_caption(ok_g, "Modelo: intensidad media por zona × kWh/TFLOP-h desde TDP y TFLOPS en `dim_gpu_model`.")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    pivot_kpi = heatmap_data.pivot(index='GPU', columns='País', values='gCO2eq_per_TFLOP') if not heatmap_data.empty else pd.DataFrame()
    best_cell = ("—", "—", float('nan'))
    worst_cell = ("—", "—", float('nan'))
    if not pivot_kpi.empty:
        arr = pivot_kpi.to_numpy(dtype=float)
        if np.isfinite(arr).any():
            bi, bj = np.unravel_index(np.nanargmin(arr), arr.shape)
            wi, wj = np.unravel_index(np.nanargmax(arr), arr.shape)
            best_cell = (pivot_kpi.index[bi], pivot_kpi.columns[bj], arr[bi, bj])
            worst_cell = (pivot_kpi.index[wi], pivot_kpi.columns[wj], arr[wi, wj])
    h100 = pivot_kpi.loc['H100'].mean() if 'H100' in pivot_kpi.index else float('nan')
    a100 = pivot_kpi.loc['A100'].mean() if 'A100' in pivot_kpi.index else float('nan')
    gap = (1.0 - h100 / a100) * 100.0 if pd.notna(h100) and pd.notna(a100) and a100 else float('nan')
    
    with col1:
        st.metric("🟢 Mejor Combinación", f"{best_cell[0]} + {best_cell[1]}")
    
    with col2:
        st.metric("🔴 Peor Combinación", f"{worst_cell[0]} + {worst_cell[1]}")
    
    with col3:
        st.metric("📊 Brecha H100 vs A100", f"{gap:.0f}%" if pd.notna(gap) else "—")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Heatmap ====================
    st.subheader("🗺️ Heatmap: Costo de Carbono por TFLOP (gCO₂eq/TFLOP-h)")
    
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
        font=dict(color="#f2f2f2")
    )
    
    fig_heatmap = apply_control_tower_plotly_theme(fig_heatmap)
    
    st.plotly_chart(fig_heatmap, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_bar, col_slope = st.columns(2)
    
    with col_bar:
        st.subheader("📊 Bar Chart: Comparación de GPUs en un País")
        
        # Filtrar por país seleccionado
        paises = sorted(heatmap_data['País'].unique().tolist()) if not heatmap_data.empty else ['SE', 'NO', 'CA']
        pais_seleccionado = st.selectbox(
            "Seleccionar País/Zona",
            options=paises,
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
            font=dict(color="#f2f2f2")
        )
        fig_bar = apply_control_tower_plotly_theme(fig_bar)
        st.plotly_chart(fig_bar, width='stretch')
    
    with col_slope:
        st.subheader("📈 Slope Chart: Una Misma GPU entre Países")
        
        pivot_sl = heatmap_data.pivot(index='GPU', columns='País', values='gCO2eq_per_TFLOP')
        if not pivot_sl.empty and 'H100' in pivot_sl.index and 'A100' in pivot_sl.index:
            slope_data = pd.DataFrame({
                'País': list(pivot_sl.columns),
                'H100': pivot_sl.loc['H100'].values,
                'A100': pivot_sl.loc['A100'].values,
            })
        else:
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
            font=dict(color="#f2f2f2"),
            legend=dict(orientation="h", y=1.1)
        )
        fig_slope = apply_control_tower_plotly_theme(fig_slope)
        st.plotly_chart(fig_slope, width='stretch')
    
    # ==================== TABLA COMPACTA ====================
    st.markdown("---")
    st.subheader("📋 Tabla de Valores Exactos")
    
    # Tabla formateada
    table_display = heatmap_data.pivot(index='GPU', columns='País', values='gCO2eq_per_TFLOP')
    tbl = table_display.astype(float)
    if np.isfinite(tbl.to_numpy()).any():
        st.dataframe(
            tbl.style.background_gradient(cmap='Greens', axis=None),
            width='stretch',
        )
    else:
        st.dataframe(table_display, width='stretch')
    
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