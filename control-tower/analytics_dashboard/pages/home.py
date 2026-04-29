"""
Página de Portada del Dashboard Green AI.
Muestra KPIs generales y overview de la plataforma.
"""
import streamlit as st
import pandas as pd
import plotly.express as px

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from gold_analytics import (
    load_gold_bundle,
    normalize_usage,
    page4_macro_scatter,
)


def render():
    """Renderizar página de portada."""

    st.markdown(
        '<p class="ad-home-title">🌱 Green AI Analytics Platform</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="ad-home-subtitle">Dashboard de sostenibilidad para cargas de IA en la nube</p>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    from_gold = False
    try:
        bundle = load_gold_bundle()
        df_usage = bundle["usage"]
        df_carbon = bundle["carbon"]
        df_country = bundle["country"]
        if df_usage.empty and df_carbon.empty:
            st.info(
                "📌 Mostrando datos de demostración: no se encontraron tablas Gold en S3 "
                "o están vacías."
            )
            df_usage = get_demo_usage_data()
            df_carbon = get_demo_carbon_data()
            df_country = get_demo_country_data()
        else:
            from_gold = True
            df_usage = normalize_usage(df_usage)
            if not df_usage.empty:
                from_gold = True
    except Exception as e:
        st.warning(f"Usando datos de demostración: {e}")
        df_usage = get_demo_usage_data()
        df_carbon = get_demo_carbon_data()
        df_country = get_demo_country_data()

    # Quitado: leyenda de fuente en portada
    # data_source_caption(from_gold, "KPIs y gráficos inferidos desde Parquet Gold cuando hay columnas esperadas.")

    col1, col2, col3, col4 = st.columns(4)

    total_energy = (
        df_usage["energy_consumed_kwh"].sum()
        if not df_usage.empty and "energy_consumed_kwh" in df_usage.columns
        else 125000
    )
    if not df_usage.empty and "co2_emissions_kg" in df_usage.columns:
        total_emissions = df_usage["co2_emissions_kg"].sum() / 1000.0
    else:
        total_emissions = total_energy * 0.35
    total_cost = (
        df_usage["cost_electricity_usd"].sum()
        if not df_usage.empty and "cost_electricity_usd" in df_usage.columns
        else 45000
    )
    countries_count = (
        len(df_country)
        if not df_country.empty
        else (df_usage["country_iso"].nunique() if "country_iso" in df_usage.columns else 45)
    )

    with col1:
        st.metric(label="⚡ Energía Total Analizada", value=f"{total_energy:,.0f} kWh", delta=None)

    with col2:
        st.metric(label="🌍 Emisiones Estimadas", value=f"{total_emissions:,.0f} tCO₂eq", delta=None)

    with col3:
        st.metric(label="💵 Costo Eléctrico Estimado", value=f"${total_cost:,.0f}", delta=None)

    with col4:
        st.metric(label="🌎 Países/Regiones Cubiertos", value=str(countries_count), delta=None)

    st.markdown("---")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📊 Distribución por Tipo de Carga")
        if not df_usage.empty and "job_type" in df_usage.columns and "energy_consumed_kwh" in df_usage.columns:
            if "session_id" in df_usage.columns:
                job_type_dist = (
                    df_usage.groupby("job_type")
                    .agg({"energy_consumed_kwh": "sum", "session_id": "count"})
                    .reset_index()
                )
                job_type_dist.columns = ["job_type", "energy_kwh", "sessions"]
            else:
                job_type_dist = df_usage.groupby("job_type", as_index=False).agg(
                    energy_kwh=("energy_consumed_kwh", "sum")
                )

            fig = px.bar(
                job_type_dist,
                x="job_type",
                y="energy_kwh",
                color="job_type",
                title="Energía por Tipo de Job",
                color_discrete_sequence=px.colors.qualitative.Plotly,
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No hay datos de uso de cómputo disponibles")

    with col_right:
        st.subheader("🌍 Intensidad de Carbono por Región")
        if not df_carbon.empty and "zone" in df_carbon.columns:
            ci_col = "carbon_intensity_gco2eq_per_kwh"
            if ci_col not in df_carbon.columns:
                alt = [c for c in df_carbon.columns if "carbon" in c.lower() and "intensity" in c.lower()]
                ci_col = alt[0] if alt else df_carbon.columns[0]
            carbon_by_region = df_carbon.groupby("zone").agg({ci_col: "mean"}).reset_index()
            carbon_by_region.columns = ["zone", "avg_carbon_intensity"]
            carbon_by_region = carbon_by_region.sort_values("avg_carbon_intensity", ascending=True).head(10)

            fig = px.bar(
                carbon_by_region,
                x="avg_carbon_intensity",
                y="zone",
                orientation="h",
                title="Intensidad de Carbono (gCO₂eq/kWh)",
                color="avg_carbon_intensity",
                color_continuous_scale="Greens",
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No hay datos de intensidad de carbono disponibles")

    st.markdown("---")
    st.subheader("🌐 Intensidad de carbono vs métrica económica (Gold)")

    try:
        bundle = load_gold_bundle()
        macro, ok_macro, _ = page4_macro_scatter(bundle)
    except Exception:
        macro, ok_macro = pd.DataFrame(), False

    if ok_macro and not macro.empty:
        fig = px.scatter(
            macro,
            x="Exportaciones_TIC_MM",
            y="Intensidad_Carbono",
            color="Región",
            hover_name="País",
            size="Low_Carbon_Share",
            title="Países: intensidad de red vs exportaciones TIC / PIB (proxy)",
            labels={
                "Exportaciones_TIC_MM": "Exportaciones TIC (MUSD) o proxy",
                "Intensidad_Carbono": "Intensidad (gCO₂eq/kWh)",
            },
        )
        st.plotly_chart(fig, width="stretch")
    else:
        ranking_data = pd.DataFrame(
            {
                "country": ["Noruega", "Suecia", "Canadá", "Brasil", "México", "EEUU"],
                "carbon_intensity": [20, 35, 45, 55, 380, 400],
                "low_carbon_share": [98, 95, 92, 78, 45, 35],
            }
        )
        fig = px.scatter(
            ranking_data,
            x="country",
            y="carbon_intensity",
            size="low_carbon_share",
            color="carbon_intensity",
            color_continuous_scale="Greens",
            title="Demostración (sin join país–zona en Gold)",
        )
        st.plotly_chart(fig, width="stretch")

    st.markdown("---")
    st.info(
        "ℹ️ **Nota:** Las tablas se leen desde el bucket configurado en `S3_GOLD_BUCKET` "
        "(credenciales en `.env`). La portada usa `load_gold_bundle()` con caché de 10 minutos."
    )


def get_demo_usage_data():
    """Generar datos de demostración para uso de cómputo."""
    return pd.DataFrame(
        {
            "session_id": [f"session_{i}" for i in range(100)],
            "job_type": ["Training"] * 40 + ["Inference"] * 35 + ["Fine-tuning"] * 25,
            "execution_status": ["Success"] * 80 + ["Failed"] * 20,
            "region": ["us-east-1"] * 30 + ["us-west-2"] * 25 + ["eu-west-1"] * 25 + ["ap-southeast-1"] * 20,
            "gpu_model": ["A100"] * 35 + ["H100"] * 30 + ["V100"] * 20 + ["T4"] * 15,
            "energy_consumed_kwh": [100 + i * 10 for i in range(100)],
            "cost_electricity_usd": [50 + i * 5 for i in range(100)],
            "duration_hours": [1 + i * 0.1 for i in range(100)],
        }
    )


def get_demo_carbon_data():
    """Generar datos de demostración para intensidad de carbono."""
    zones = ["US-CA", "US-TX", "US-NY", "BR", "MX", "AR", "CL", "CO", "DE", "FR"]
    data = []
    for zone in zones:
        for hour in range(24):
            data.append(
                {
                    "zone": zone,
                    "hour": hour,
                    "carbon_intensity_gco2eq_per_kwh": 50 + (hour % 12) * 30 + (hash(zone) % 100),
                }
            )
    return pd.DataFrame(data)


def get_demo_country_data():
    """Generar datos de demostración para países."""
    return pd.DataFrame(
        {
            "country_id": ["USA", "CAN", "MEX", "BRA", "ARG", "CHL", "COL", "PER"],
            "country_name": [
                "Estados Unidos",
                "Canadá",
                "México",
                "Brasil",
                "Argentina",
                "Chile",
                "Colombia",
                "Perú",
            ],
            "iso_alpha3": ["USA", "CAN", "MEX", "BRA", "ARG", "CHL", "COL", "PER"],
            "region": [
                "Norteamérica",
                "Norteamérica",
                "Latinoamérica",
                "Latinoamérica",
                "Latinoamérica",
                "Latinoamérica",
                "Latinoamérica",
                "Latinoamérica",
            ],
            "income_group": ["Alto", "Alto", "Medio-Alto", "Medio-Alto", "Medio-Alto", "Alto", "Medio", "Medio"],
        }
    )


if __name__ == "__main__":
    render()
