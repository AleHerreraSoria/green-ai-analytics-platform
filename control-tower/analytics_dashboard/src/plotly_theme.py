"""
Tema Plotly alineado con Control Tower (.streamlit/config.toml y main.py).
Fondo transparente para integrarse con el tema oscuro de Streamlit.
"""

from __future__ import annotations

# Alineado con theme.textColor y acentos de la app principal
TEXT_COLOR = "#f2f2f2"
GRID_COLOR = "rgba(242, 242, 242, 0.22)"
TRANSPARENT = "rgba(0, 0, 0, 0)"
# Tinte tierra (#deff9a) coherente con primaryColor — sustituye el verde bosque anterior
GEO_LAND_COLOR = "rgba(222, 255, 154, 0.12)"


def apply_control_tower_plotly_theme(fig):
    """Estilo común para figuras Plotly sobre el fondo oscuro del Control Tower."""

    text_color = TEXT_COLOR
    grid_color = GRID_COLOR
    transparent = TRANSPARENT

    fig.update_layout(
        font=dict(color=text_color),
        title=dict(
            font=dict(color=text_color, size=15),
            x=0,
            xanchor="left",
            y=0.98,
            yanchor="top",
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
            tracegroupgap=6,
        ),
    )

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
        linecolor=grid_color,
    )

    fig.update_yaxes(
        title_font=dict(color=text_color),
        tickfont=dict(color=text_color),
        color=text_color,
        gridcolor=grid_color,
        zerolinecolor=grid_color,
        linecolor=grid_color,
    )

    fig.update_annotations(font=dict(color=text_color))

    try:
        fig.update_geos(
            bgcolor=transparent,
            lakecolor=transparent,
            landcolor=GEO_LAND_COLOR,
            coastlinecolor=grid_color,
            framecolor=grid_color,
        )
    except Exception:
        pass

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
