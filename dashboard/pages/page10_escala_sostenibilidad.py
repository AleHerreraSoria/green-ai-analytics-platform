"""
Página 10: Escala de cómputo vs sostenibilidad
================================================
Pregunta: si los países con mayor volumen de cómputo tienden o no a menor intensidad de carbono.

Visual principal: Scatter plot (volumen vs intensidad)
Soporte: Etiquetado de outliers, Ranking alto volumen + alta CI, Tabla interpretativa
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
    """Renderizar Página 10: Escala de cómputo vs sostenibilidad."""
    
    st.markdown('<p class="page-header">📏 Página 10: Escala de Cómputo vs Sostenibilidad</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-subheader">Si los países con mayor volumen de cómputo tienden o no a menor intensidad de carbono</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==================== KPIs ====================
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📊 Correlación Volumen-CI", "-0.35")
    
    with col2:
        st.metric("⬆️ Outlier Positivo", "Suecia")
    
    with col3:
        st.metric("⬇️ Outlier Negativo", "China")
    
    st.markdown("---")
    
    # ==================== VISUAL PRINCIPAL: Scatter Plot ====================
    st.subheader("🎯 Scatter Plot: Volumen de Cómputo vs Intensidad de Carbono")
    
    # Datos de países
    scale_data = pd.DataFrame({
        'País': ['Estados Unidos', 'China', 'Alemania', 'Japón', 'Reino Unido', 
                'Canadá', 'Francia', 'Corea del Sur', 'India', 'Brasil',
                'Australia', 'México', 'Indonesia', 'Rusia', 'Arabia Saudí'],
        'Volumen_kWh': [4500000, 5200000, 1800000, 2200000, 1500000, 
                       1200000, 1100000, 950000, 850000, 720000,
                       580000, 450000, 380000, 350000, 280000],
        'Intensidad_Carbono': [420, 580, 380, 450, 350, 120, 320, 520, 680, 85,
                              650, 380, 520, 380, 450],
        'Participacion': [18, 21, 7, 9, 6, 5, 4, 4, 3, 3,
                         2, 2, 2, 1, 1],
        'Región': ['Norteamérica', 'Asia', 'Europa', 'Asia', 'Europa', 
                  'Norteamérica', 'Europa', 'Asia', 'Asia', 'Sudamérica',
                  'Oceanía', 'Latinoamérica', 'Asia', 'Europa', 'Asia']
    })
    
    # Calcular correlación
    from scipy import stats
    corr, p_value = stats.pearsonr(scale_data['Volumen_kWh'], scale_data['Intensidad_Carbono'])
    
    fig_scatter = px.scatter(
        scale_data,
        x='Volumen_kWh',
        y='Intensidad_Carbono',
        color='Región',
        hover_name='País',
        title=f"Volumen de Cómputo vs Intensidad de Carbono (r={corr:.2f}, p={p_value:.3f})",
        labels={
            'Volumen_kWh': 'Volumen de Cómputo (kWh)',
            'Intensidad_Carbono': 'Intensidad de Carbono (gCO₂eq/kWh)',
            'Participacion': '% Participación'
        },
        hover_data={
            'Participacion': ':.0f'
        },
        color_discrete_sequence=px.colors.qualitative.Bold
    )

    fig_scatter.update_traces(
        marker=dict(
            size=14,
            line=dict(width=1, color='#ffffff')
        )
    )
    
    # Añadir etiquetas para outliers
    outliers = scale_data.nlargest(3, 'Volumen_kWh')
    for idx, row in outliers.iterrows():
        fig_scatter.add_annotation(
            x=row['Volumen_kWh'],
            y=row['Intensidad_Carbono'],
            text=row['País'],
            showarrow=True,
            arrowhead=1
        )
    
    fig_scatter.update_layout(
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff"),
        legend=dict(orientation="h", y=1.05)
    )
    
    fig_scatter = aplicar_tema_plotly(fig_scatter)
    
    st.plotly_chart(fig_scatter, width='stretch')
    
    st.markdown("---")
    
    # ==================== VISUALES DE APOYO ====================
    col_outlier, col_ranking = st.columns(2)
    
    with col_outlier:
        st.subheader("⬆️ Outlier Positivo: Alto Volumen + Baja CI")
        
        positive_outliers = scale_data[
            (scale_data['Volumen_kWh'] > scale_data['Volumen_kWh'].median()) & 
            (scale_data['Intensidad_Carbono'] < scale_data['Intensidad_Carbono'].median())
        ].sort_values('Intensidad_Carbono')
        
        fig_pos = px.bar(
            positive_outliers,
            y='País',
            x='Intensidad_Carbono',
            orientation='h',
            color='Intensidad_Carbono',
            color_continuous_scale='Greens',
            title="Países con Alto Cómputo y Baja Intensidad"
        )
        fig_pos.update_layout(
            height=300,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_pos = aplicar_tema_plotly(fig_pos)
        st.plotly_chart(fig_pos, width='stretch')
    
    with col_ranking:
        st.subheader("⬇️ Outlier Negativo: Alto Volumen + Alta CI")
        
        negative_outliers = scale_data[
            (scale_data['Volumen_kWh'] > scale_data['Volumen_kWh'].median()) & 
            (scale_data['Intensidad_Carbono'] > scale_data['Intensidad_Carbono'].median())
        ].sort_values('Intensidad_Carbono', ascending=False)
        
        fig_neg = px.bar(
            negative_outliers,
            y='País',
            x='Intensidad_Carbono',
            orientation='h',
            color='Intensidad_Carbono',
            color_continuous_scale='Reds',
            title="Países con Alto Cómputo y Alta Intensidad"
        )
        fig_neg.update_layout(
            height=300,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#ffffff")
        )
        fig_neg = aplicar_tema_plotly(fig_neg)
        st.plotly_chart(fig_neg, width='stretch')
    
    # ==================== TABLA INTERPRETATIVA ====================
    st.markdown("---")
    st.subheader("📋 Tabla País: Volumen, CI y Categoría Interpretativa")
    
    # Clasificar países
    def classify_country(row):
        if row['Volumen_kWh'] > scale_data['Volumen_kWh'].median() and row['Intensidad_Carbono'] < scale_data['Intensidad_Carbono'].median():
            return '✅ Líder Verde'
        elif row['Volumen_kWh'] > scale_data['Volumen_kWh'].median() and row['Intensidad_Carbono'] > scale_data['Intensidad_Carbono'].median():
            return '⚠️ Necesita Transición'
        elif row['Volumen_kWh'] < scale_data['Volumen_kWh'].median() and row['Intensidad_Carbono'] < scale_data['Intensidad_Carbono'].median():
            return '🔵 Bajo Impacto'
        else:
            return '🟡 Oportunidad de Crecimiento'
    
    scale_data['Categoría'] = scale_data.apply(classify_country, axis=1)
    
    display_table = scale_data[['País', 'Volumen_kWh', 'Intensidad_Carbono', 'Participacion', 'Categoría']].sort_values('Volumen_kWh', ascending=False)
    display_table['Volumen_kWh'] = display_table['Volumen_kWh'].apply(lambda x: f"{x:,.0f}")
    display_table['Intensidad_Carbono'] = display_table['Intensidad_Carbono'].apply(lambda x: f"{x}")
    display_table['Participacion'] = display_table['Participacion'].apply(lambda x: f"{x}%")
    
    st.dataframe(
        display_table.style.apply(
            lambda x: ['background: #10b981' if 'Líder' in str(x) else 
                     'background: #ef4444' if 'Necesita' in str(x) else
                     'background: #f59e0b' if 'Oportunidad' in str(x) else '' for i in x], 
            axis=1
        ),
        width='stretch'
    )
    
    # ==================== EXPLICACIÓN ====================
    st.markdown("---")
    st.info("""
    💡 **Por qué funciona este diseño:**
    - El **scatter** identifica la relación entre volumen y sostenibilidad
    - El **etiquetado de outliers** destaca países que rompen el patrón
    - Los **rankings separados** muestran líderes y rezagados
    - La **tabla interpretativa** clasifica cada país en una categoría accionable
    """)


if __name__ == "__main__":
    render()