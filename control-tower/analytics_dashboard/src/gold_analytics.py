"""
Carga centralizada desde la capa Gold + normalización de columnas (`pick_column`).
Las páginas del dashboard consumen `load_gold_bundle()` y helpers por vista.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from s3_connection import load_dimension, load_fact_table

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Columnas candidatas (variantes típicas entre pipelines Spark / naming)
# -----------------------------------------------------------------------------


def pick_column(df: pd.DataFrame, *candidates: str) -> str | None:
    if df is None or df.empty:
        return None
    cmap = {str(c).lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in cmap:
            return cmap[key]
    return None


def normalize_usage(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren: dict[str, str] = {}
    for target, candidates in (
        ("gpu_model", ("gpu_model", "gpu_model_name", "gpu_name", "model_name")),
        ("aws_region_code", ("aws_region_code", "region", "aws_region", "destination_region", "region_code")),
        ("energy_consumed_kwh", ("energy_consumed_kwh", "energy_kwh", "kwh_consumed", "compute_energy_kwh")),
        ("cost_electricity_usd", ("cost_electricity_usd", "electricity_cost_usd", "cost_electric_usd")),
        ("duration_hours", ("duration_hours", "session_duration_h", "hours", "duration_h")),
        ("co2_emissions_kg", ("co2_emissions_kg", "carbon_emissions_kg", "emissions_kg_co2eq", "co2_kg")),
        ("cost_compute_usd", ("cost_compute_usd", "compute_cost_usd")),
        ("job_type", ("job_type", "workload_type", "job_category")),
        ("execution_status", ("execution_status", "status", "outcome", "run_status")),
        ("country_iso", ("country_iso", "iso_code", "country_code", "iso_alpha3")),
    ):
        if target not in out.columns:
            if c := pick_column(out, *candidates):
                ren[c] = target
    if ren:
        out = out.rename(columns=ren)
    return out


def normalize_carbon_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren = {}
    if "zone" not in out.columns:
        if c := pick_column(out, "electricity_maps_zone", "em_zone", "zone_code", "grid_zone"):
            ren[c] = "zone"
    if "hour" not in out.columns:
        if c := pick_column(out, "hour_of_day", "hour_local", "h"):
            ren[c] = "hour"
        elif c := pick_column(out, "timestamp", "ts", "datetime", "event_time"):
            ts = pd.to_datetime(out[c], errors="coerce")
            out["hour"] = ts.dt.hour
    if "carbon_intensity_gco2eq_per_kwh" not in out.columns:
        if c := pick_column(
            out,
            "carbon_intensity_gco2eq_per_kwh",
            "carbon_intensity_g_per_kwh",
            "grid_intensity_gco2_kwh",
            "carbon_intensity",
        ):
            ren[c] = "carbon_intensity_gco2eq_per_kwh"
    out = out.rename(columns=ren)
    return out


def normalize_dim_gpu(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren = {}
    if "gpu_model" not in out.columns:
        if c := pick_column(out, "gpu_model_name", "model_name", "gpu_name"):
            ren[c] = "gpu_model"
    if "tdp_watts" not in out.columns:
        if c := pick_column(
            out,
            "tdp_watts",
            "tdp_w",
            "thermal_design_power_w",
            "tdp",
            "gpu_tdp_watts",
            "power_watts",
        ):
            ren[c] = "tdp_watts"
    if "tflops_fp32" not in out.columns:
        # Intentar primero aliases directos
        if c := pick_column(
            out,
            "tflops_fp32",
            "tf32_tflops",
            "fp32_tflops",
            "tflops",
            "gpu_tflops_fp32",
            "peak_fp32_tflops",
            "fp32_peak_tflops",
        ):
            ren[c] = "tflops_fp32"
        # Si no existe pero existe gflops_fp32, crear tflops_fp32 con conversión defensiva
        elif "gflops_fp32" in out.columns:
            gflops_col = "gflops_fp32"
            median_val = out[gflops_col].median()
            if pd.notna(median_val) and median_val > 1000:
                # Parece estar en GFLOPS, convertir a TFLOPS
                out["tflops_fp32"] = out[gflops_col] / 1000.0
            else:
                # Usar tal cual como TFLOPS
                out["tflops_fp32"] = out[gflops_col]
    out = out.rename(columns=ren)
    return out


def normalize_dim_country(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren = {}
    if "country_name" not in out.columns:
        if c := pick_column(out, "country_name", "name", "country"):
            ren[c] = "country_name"
    if "iso_code" not in out.columns:
        if c := pick_column(out, "iso_alpha3", "iso_code", "country_iso", "iso"):
            ren[c] = "iso_code"
    if "gdp_per_capita_usd" not in out.columns:
        if c := pick_column(out, "gdp_per_capita_usd", "gdp_per_capita", "maddison_gdp_pc"):
            ren[c] = "gdp_per_capita_usd"
    if "tic_exports_musd" not in out.columns:
        if c := pick_column(
            out,
            "tic_exports_millions_usd",
            "exports_ict_million_usd",
            "service_exports_braoder_usd",
            "ict_exports_usd",
        ):
            ren[c] = "tic_exports_musd"
    if "low_carbon_share_pct" not in out.columns:
        if c := pick_column(
            out,
            "low_carbon_electricity_share_pct",
            "renewable_share_pct",
            "clean_electricity_pct",
        ):
            ren[c] = "low_carbon_share_pct"
    if "continent" not in out.columns:
        if c := pick_column(out, "continent", "region_owid"):
            ren[c] = "continent"
    out = out.rename(columns=ren)
    return out


def normalize_dim_region(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren = {}
    if "aws_region_code" not in out.columns:
        if c := pick_column(out, "aws_region_code", "region_code", "aws_region"):
            ren[c] = "aws_region_code"
    if "electricity_maps_zone" not in out.columns:
        if c := pick_column(out, "electricity_maps_zone", "zone", "em_zone"):
            ren[c] = "electricity_maps_zone"
    if "country_iso" not in out.columns:
        if c := pick_column(out, "country_iso", "iso_code", "iso_alpha3"):
            ren[c] = "country_iso"
    out = out.rename(columns=ren)
    return out


def normalize_country_energy(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    ren = {}
    # iso_code desde iso_alpha3
    if "iso_code" not in out.columns:
        if c := pick_column(out, "iso_code", "country_iso", "iso_alpha3"):
            ren[c] = "iso_code"
    # year
    if "year" not in out.columns:
        if c := pick_column(out, "year", "report_year"):
            ren[c] = "year"
    # tic_exports_musd desde ict_exports_usd con conversión a millones
    if "tic_exports_musd" not in out.columns:
        if c := pick_column(out, "tic_exports_musd", "exports_ict_million_usd"):
            ren[c] = "tic_exports_musd"
        elif "ict_exports_usd" in out.columns:
            # ict_exports_usd puede estar en USD o millones
            median_val = out["ict_exports_usd"].median()
            if pd.notna(median_val) and median_val > 1000:
                # Parece estar en USD, convertir a millones
                out["tic_exports_musd"] = out["ict_exports_usd"] / 1_000_000
            else:
                # Ya parece estar en millones
                out["tic_exports_musd"] = out["ict_exports_usd"]
    # carbon_intensity_gco2eq_per_kwh desde carbon_intensity_elec
    if "carbon_intensity_gco2eq_per_kwh" not in out.columns:
        if c := pick_column(out, "carbon_intensity_gco2eq_per_kwh", "grid_carbon_intensity"):
            ren[c] = "carbon_intensity_gco2eq_per_kwh"
        elif "carbon_intensity_elec" in out.columns:
            out["carbon_intensity_gco2eq_per_kwh"] = out["carbon_intensity_elec"]
    # gdp_per_capita_usd desde gdp_per_capita
    if "gdp_per_capita_usd" not in out.columns:
        if c := pick_column(out, "gdp_per_capita_usd", "gdp_per_capita"):
            ren[c] = "gdp_per_capita_usd"
    # low_carbon_share_pct desde low_carbon_share_elec
    if "low_carbon_share_pct" not in out.columns:
        if c := pick_column(out, "low_carbon_share_pct", "low_carbon_share_elec"):
            ren[c] = "low_carbon_share_pct"
    out = out.rename(columns=ren)
    return out


def _estimate_co2_kg(row: pd.Series, intensity_g_per_kwh: float | None) -> float:
    eng = row.get("energy_consumed_kwh")
    if pd.isna(eng) or intensity_g_per_kwh is None:
        return np.nan
    return float(eng) * float(intensity_g_per_kwh) / 1000.0


def enrich_usage_emissions(
    usage: pd.DataFrame,
    carbon: pd.DataFrame,
    region: pd.DataFrame,
) -> pd.DataFrame:
    """Añade zone, intensidad media y co2 estimado si falta columna de emisiones."""
    u = usage.copy()
    if u.empty:
        return u
    if carbon.empty or not {"zone", "carbon_intensity_gco2eq_per_kwh"}.issubset(carbon.columns):
        return u
    zone_ci = carbon.groupby("zone", as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
    zm = dict(zip(zone_ci["zone"], zone_ci["carbon_intensity_gco2eq_per_kwh"]))
    if not region.empty and {"aws_region_code", "electricity_maps_zone"}.issubset(region.columns):
        rm = dict(zip(region["aws_region_code"], region["electricity_maps_zone"]))
        u["zone"] = u["aws_region_code"].map(rm) if "aws_region_code" in u.columns else np.nan
    else:
        u["zone"] = u["aws_region_code"] if "aws_region_code" in u.columns else np.nan
    u["_ci"] = u["zone"].map(zm)
    if "co2_emissions_kg" not in u.columns or u["co2_emissions_kg"].isna().all():
        u["co2_emissions_kg"] = u.apply(lambda r: _estimate_co2_kg(r, r.get("_ci")), axis=1)
    u = u.drop(columns=["_ci"], errors="ignore")
    return u


# --- Americas / Latam ISO3 (subset) -------------------------------------------

AMERICAS_ISO3 = frozenset({
    "ARG", "ATG", "BHS", "BRB", "BLZ", "BOL", "BRA", "CAN", "CHL", "COL", "CRI", "CUB",
    "DMA", "DOM", "ECU", "SLV", "GTM", "GUY", "HTI", "HND", "JAM", "MEX", "NIC", "PAN",
    "PRY", "PER", "KNA", "LCA", "VCT", "SUR", "TTO", "USA", "URY", "VEN", "PER", "MEX",
})

LATAM_ISO3 = frozenset({
    "ARG", "BOL", "BRA", "CHL", "COL", "CRI", "CUB", "DOM", "ECU", "SLV", "GTM", "HND",
    "MEX", "NIC", "PAN", "PRY", "PER", "URY", "VEN",
})


@st.cache_data(ttl=600, show_spinner="Cargando tablas Gold desde S3…")
def load_gold_bundle() -> dict[str, pd.DataFrame]:
    """Carga y normaliza todas las tablas Gold usadas por los dashboards."""
    raw_usage = load_fact_table("fact_ai_compute_usage")
    raw_carbon = load_fact_table("fact_carbon_intensity_hourly")
    raw_country_e = load_fact_table("fact_country_energy_annual")
    raw_country = load_dimension("dim_country")
    raw_region = load_dimension("dim_region")
    raw_gpu = load_dimension("dim_gpu_model")

    usage = normalize_usage(raw_usage)
    carbon = normalize_carbon_hourly(raw_carbon)
    country_e = normalize_country_energy(raw_country_e)
    country = normalize_dim_country(raw_country)
    region = normalize_dim_region(raw_region)
    gpu = normalize_dim_gpu(raw_gpu)

    if not usage.empty and not carbon.empty:
        usage = enrich_usage_emissions(usage, carbon, region)

    if not usage.empty and not region.empty:
        if "aws_region_code" in usage.columns and "country_iso" in region.columns:
            rsub = region.drop_duplicates(subset=["aws_region_code"])[
                ["aws_region_code", "country_iso"]
            ]
            usage = usage.merge(rsub, on="aws_region_code", how="left", suffixes=("", "_r"))
            if "country_iso_r" in usage.columns:
                if "country_iso" in usage.columns:
                    usage["country_iso"] = usage["country_iso"].fillna(usage["country_iso_r"])
                else:
                    usage["country_iso"] = usage["country_iso_r"]
                usage.drop(columns=["country_iso_r"], inplace=True, errors="ignore")
            if "country_iso_y" in usage.columns:
                if "country_iso_x" in usage.columns:
                    usage["country_iso"] = usage["country_iso_x"].combine_first(usage["country_iso_y"])
                else:
                    usage["country_iso"] = usage["country_iso_y"]
                usage.drop(
                    columns=[c for c in ("country_iso_x", "country_iso_y") if c in usage.columns],
                    inplace=True,
                    errors="ignore",
                )

    return {
        "usage": usage,
        "carbon": carbon,
        "country_energy": country_e,
        "country": country,
        "region": region,
        "gpu": gpu,
    }


def bundle_has_usage(bundle: dict[str, pd.DataFrame]) -> bool:
    return not bundle["usage"].empty


def bundle_has_carbon(bundle: dict[str, pd.DataFrame]) -> bool:
    c = bundle["carbon"]
    return not c.empty and "carbon_intensity_gco2eq_per_kwh" in c.columns


def data_source_caption(from_gold: bool, detail: str = "") -> None:
    if from_gold:
        st.caption("Fuente: **capa Gold (S3)** — " + (detail or "Parquet agregado en la app."))
    else:
        st.caption("⚠️ **Sin datos suficientes en Gold**; se muestran valores de ejemplo." + (f" {detail}" if detail else ""))


# --- Page builders -----------------------------------------------------------


def page2_gpu_table(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    u, g = bundle["usage"], bundle["gpu"]
    if u.empty or "gpu_model" not in u.columns:
        return pd.DataFrame(), False
    u = u.copy()
    dur = u["duration_hours"].replace(0, np.nan) if "duration_hours" in u.columns else pd.Series(np.nan, index=u.index)
    u["costo_hora"] = np.nan
    u["emisiones_hora_kg"] = np.nan
    if "cost_electricity_usd" in u.columns and dur.notna().any():
        u["costo_hora"] = u["cost_electricity_usd"] / dur
    if "co2_emissions_kg" in u.columns and dur.notna().any():
        u["emisiones_hora_kg"] = u["co2_emissions_kg"] / dur
    agg_dict: dict[str, Any] = {}
    if u["costo_hora"].notna().any():
        agg_dict["costo_hora"] = ("costo_hora", "mean")
    if u["emisiones_hora_kg"].notna().any():
        agg_dict["emisiones_hora_kg"] = ("emisiones_hora_kg", "mean")
    if not agg_dict:
        return pd.DataFrame(), False
    grp = u.groupby("gpu_model", as_index=False).agg(**{k: v for k, v in agg_dict.items()})
    if not g.empty and "gpu_model" in g.columns:
        extra = [c for c in ("tflops_fp32", "tdp_watts") if c in g.columns]
        merge_cols = ["gpu_model"] + extra
        g_small = g[merge_cols].drop_duplicates(subset=["gpu_model"])
        grp = grp.merge(g_small, on="gpu_model", how="left")
    for col in ("tflops_fp32", "tdp_watts"):
        if col not in grp.columns:
            grp[col] = np.nan
    grp = grp.rename(columns={"gpu_model": "GPU"})
    return grp, True


def page3_tflop_matrix(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    carbon, gpu = bundle["carbon"], bundle["gpu"]
    if carbon.empty or gpu.empty:
        return pd.DataFrame(), False
    if not {"zone", "carbon_intensity_gco2eq_per_kwh"}.issubset(carbon.columns):
        return pd.DataFrame(), False
    if "gpu_model" not in gpu.columns:
        return pd.DataFrame(), False
    zi = carbon.groupby("zone", as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
    rows = []
    for _, gr in gpu.drop_duplicates(subset=["gpu_model"]).iterrows():
        name = gr["gpu_model"]
        tdp = (
            float(gr["tdp_watts"])
            if "tdp_watts" in gr.index and pd.notna(gr["tdp_watts"])
            else np.nan
        )
        tf = (
            float(gr["tflops_fp32"])
            if "tflops_fp32" in gr.index and pd.notna(gr["tflops_fp32"])
            else np.nan
        )
        if not tdp or not tf or tf <= 0:
            continue
        kwh_per_tflop_h = (tdp / 1000.0) / tf
        for _, zr in zi.iterrows():
            zone = zr["zone"]
            ci = float(zr["carbon_intensity_gco2eq_per_kwh"])
            gco2_tflop_h = ci * kwh_per_tflop_h
            rows.append({"GPU": name, "País": str(zone), "gCO2eq_per_TFLOP": gco2_tflop_h})
    if not rows:
        return pd.DataFrame(), False
    out_df = pd.DataFrame(rows)
    out_df = out_df[np.isfinite(out_df["gCO2eq_per_TFLOP"].astype(float))]
    if out_df.empty:
        return pd.DataFrame(), False
    return out_df, True


def page4_macro_scatter(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool, str]:
    """Exportaciones TIC (o PIB) vs intensidad de carbono por país."""
    ctry, ce, carbon, region = (
        bundle["country"],
        bundle["country_energy"],
        bundle["carbon"],
        bundle["region"],
    )
    note = ""
    if not carbon.empty and "zone" in carbon.columns:
        cz = carbon.groupby("zone", as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
    else:
        cz = pd.DataFrame()

    rows = []
    if not ctry.empty and "iso_code" in ctry.columns:
        for _, r in ctry.iterrows():
            iso = str(r["iso_code"]).strip()
            name = r.get("country_name", iso)
            gdp = r.get("gdp_per_capita_usd", np.nan)
            tic = r.get("tic_exports_musd", np.nan)
            low = r.get("low_carbon_share_pct", np.nan)
            ci_v = np.nan
            if not cz.empty:
                z = None
                if not region.empty:
                    m = region[region["country_iso"] == iso] if "country_iso" in region.columns else pd.DataFrame()
                    if not m.empty and "electricity_maps_zone" in m.columns:
                        z = m["electricity_maps_zone"].iloc[0]
                if z is not None:
                    hit = cz[cz["zone"] == z]
                    if not hit.empty:
                        ci_v = hit["carbon_intensity_gco2eq_per_kwh"].iloc[0]
            x = tic if pd.notna(tic) else gdp
            if pd.notna(x) and pd.notna(ci_v):
                rows.append(
                    {
                        "iso_code": iso,
                        "País": name,
                        "Exportaciones_TIC_MM": float(x),
                        "Intensidad_Carbono": float(ci_v),
                        "Low_Carbon_Share": float(low) if pd.notna(low) else 50.0,
                        "Región": str(r.get("continent", "—")),
                    }
                )
        if rows:
            note = "Eje X: exportaciones TIC (MUSD) o PIB per cápita como proxy si no hay TIC; eje Y: intensidad media por zona."

    if not rows and not ce.empty and {"iso_code", "year"}.issubset(ce.columns):
        ce2 = ce.sort_values("year").groupby("iso_code", as_index=False).last()
        for _, r in ce2.iterrows():
            iso = r["iso_code"]
            tic = r.get("tic_exports_musd", r.get("gdp_per_capita_usd", np.nan))
            ci_v = r.get("carbon_intensity_gco2eq_per_kwh", np.nan)
            if pd.notna(tic) and pd.notna(ci_v):
                rows.append(
                    {
                        "iso_code": str(iso),
                        "País": str(iso),
                        "Exportaciones_TIC_MM": float(tic),
                        "Intensidad_Carbono": float(ci_v),
                        "Low_Carbon_Share": 50.0,
                        "Región": "—",
                    }
                )
        note = "Serie `fact_country_energy_annual`."

    if not rows:
        return pd.DataFrame(), False, note
    return pd.DataFrame(rows), True, note


def page5_americas(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    """
    Página 5: PIB per cápita vs intensidad de carbono en América.

    Prioriza fact_country_energy_annual porque ahí está el PIB per cápita real.
    Usa page4_macro_scatter solo como fallback.
    """
    ce = bundle["country_energy"]
    ctry = bundle["country"]

    required = {
        "iso_code",
        "year",
        "gdp_per_capita_usd",
        "carbon_intensity_gco2eq_per_kwh",
    }

    if not ce.empty and required.issubset(ce.columns):
        ce2 = ce.copy()

        ce2["iso_code"] = ce2["iso_code"].astype(str).str.upper().str.strip()
        ce2 = ce2[ce2["iso_code"].isin(AMERICAS_ISO3)].copy()

        ce2["gdp_per_capita_usd"] = pd.to_numeric(
            ce2["gdp_per_capita_usd"],
            errors="coerce",
        )
        ce2["carbon_intensity_gco2eq_per_kwh"] = pd.to_numeric(
            ce2["carbon_intensity_gco2eq_per_kwh"],
            errors="coerce",
        )

        if "low_carbon_share_pct" in ce2.columns:
            ce2["low_carbon_share_pct"] = pd.to_numeric(
                ce2["low_carbon_share_pct"],
                errors="coerce",
            )
        else:
            ce2["low_carbon_share_pct"] = np.nan

        ce2 = ce2.dropna(
            subset=[
                "iso_code",
                "year",
                "gdp_per_capita_usd",
                "carbon_intensity_gco2eq_per_kwh",
            ]
        )

        if not ce2.empty:
            idx = ce2.groupby("iso_code")["year"].idxmax()
            latest = ce2.loc[idx].copy()

            name_map = {}
            region_map = {}

            if not ctry.empty and "iso_code" in ctry.columns:
                ctmp = ctry.copy()
                ctmp["iso_code"] = ctmp["iso_code"].astype(str).str.upper().str.strip()

                if "country_name" in ctmp.columns:
                    name_map = dict(zip(ctmp["iso_code"], ctmp["country_name"]))

                if "region" in ctmp.columns:
                    region_map = dict(zip(ctmp["iso_code"], ctmp["region"]))

            latest["País"] = latest["iso_code"].map(
                lambda iso: name_map.get(str(iso), str(iso))
            )
            latest["PIB_per_capita"] = latest["gdp_per_capita_usd"]
            latest["Intensidad_Carbono"] = latest["carbon_intensity_gco2eq_per_kwh"]
            latest["Low_Carbon_Share"] = latest["low_carbon_share_pct"].fillna(50.0)
            latest["Región"] = latest["iso_code"].map(
                lambda iso: region_map.get(str(iso), "América")
            )
            latest["Subregión"] = latest["Región"]

            cols = [
                "iso_code",
                "País",
                "PIB_per_capita",
                "Intensidad_Carbono",
                "Low_Carbon_Share",
                "Región",
                "Subregión",
            ]

            if "tic_exports_musd" in latest.columns:
                latest["Exportaciones_TIC_MM"] = latest["tic_exports_musd"]
                cols.append("Exportaciones_TIC_MM")

            return latest[cols].reset_index(drop=True), True

    # Fallback: usar salida macro si no se puede armar desde country_energy.
    df, ok, _ = page4_macro_scatter(bundle)

    if df.empty or not ok:
        return pd.DataFrame(), False

    if "iso_code" not in df.columns:
        return df, True

    sub = df[df["iso_code"].astype(str).str.upper().isin(AMERICAS_ISO3)].copy()

    if sub.empty:
        return df, True

    if "PIB_per_capita" not in sub.columns and "Exportaciones_TIC_MM" in sub.columns:
        sub["PIB_per_capita"] = sub["Exportaciones_TIC_MM"]

    if "Subregión" not in sub.columns:
        sub["Subregión"] = sub["Región"] if "Región" in sub.columns else "América"

    return sub, True


def page6_latam_scenario(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    u, carbon, region = bundle["usage"], bundle["carbon"], bundle["region"]
    if u.empty or "energy_consumed_kwh" not in u.columns:
        return pd.DataFrame(), False
    if region.empty or not {"aws_region_code", "country_iso"}.issubset(region.columns):
        return pd.DataFrame(), False
    if carbon.empty or "zone" not in carbon.columns:
        return pd.DataFrame(), False
    zm = carbon.groupby("zone")["carbon_intensity_gco2eq_per_kwh"].mean().to_dict()
    rm = region.drop_duplicates(subset=["aws_region_code"])
    iso_like = [
        c
        for c in u.columns
        if str(c).strip().lower() == "country_iso" or str(c).strip().lower().startswith("country_iso_")
    ]
    u_base = u.drop(columns=iso_like, errors="ignore")
    u2 = u_base.merge(
        rm[["aws_region_code", "country_iso"]],
        on="aws_region_code",
        how="left",
        suffixes=("", "_r"),
    )
    if "country_iso_r" in u2.columns:
        if "country_iso" in u2.columns:
            u2["country_iso"] = u2["country_iso"].combine_first(u2["country_iso_r"])
        else:
            u2["country_iso"] = u2["country_iso_r"]
        u2.drop(columns=["country_iso_r"], inplace=True, errors="ignore")
    if "country_iso_y" in u2.columns:
        if "country_iso_x" in u2.columns:
            u2["country_iso"] = u2["country_iso_x"].combine_first(u2["country_iso_y"])
        else:
            u2["country_iso"] = u2["country_iso_y"]
        u2.drop(
            columns=[c for c in ("country_iso_x", "country_iso_y") if c in u2.columns],
            inplace=True,
            errors="ignore",
        )
    if "country_iso" not in u2.columns:
        return pd.DataFrame(), False
    u2 = u2[u2["country_iso"].astype(str).str.upper().isin(LATAM_ISO3)]
    if u2.empty:
        return pd.DataFrame(), False
    int_by_country: dict[str, float] = {}
    for iso in u2["country_iso"].dropna().unique():
        zs = (
            region[region["country_iso"] == iso]["electricity_maps_zone"].dropna().unique()
            if "electricity_maps_zone" in region.columns
            else []
        )
        vals = [zm.get(z) for z in zs if z in zm]
        int_by_country[str(iso)] = float(np.nanmean(vals)) if vals else np.nan
    g = u2.groupby("country_iso", as_index=False).agg(Base_kWh=("energy_consumed_kwh", "sum"))
    g["Intensidad_Carbono"] = g["country_iso"].astype(str).map(int_by_country)
    g["País"] = g["country_iso"]
    g["Incremento_kWh"] = g["Base_kWh"] * 0.2
    g = g.dropna(subset=["Intensidad_Carbono"])
    if g.empty:
        return pd.DataFrame(), False
    return g, True


def page8_blindspots(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    u, carbon, region = bundle["usage"], bundle["carbon"], bundle["region"]
    if u.empty or carbon.empty:
        return pd.DataFrame(), False
    if "aws_region_code" not in u.columns or "energy_consumed_kwh" not in u.columns:
        return pd.DataFrame(), False
    vol = u.groupby("aws_region_code", as_index=False).agg(Demanda_Tecnologica=("energy_consumed_kwh", "sum"))
    vol["Demanda_Tecnologica"] = vol["Demanda_Tecnologica"] / max(vol["Demanda_Tecnologica"].max(), 1.0) * 100.0
    zm = carbon.groupby("zone")["carbon_intensity_gco2eq_per_kwh"].mean().to_dict()
    if not region.empty and "electricity_maps_zone" in region.columns:
        vol["zone"] = vol["aws_region_code"].map(dict(zip(region["aws_region_code"], region["electricity_maps_zone"])))
    else:
        vol["zone"] = vol["aws_region_code"]
    vol["Intensidad_Carbono"] = vol["zone"].map(zm)
    vol = vol.dropna(subset=["Intensidad_Carbono"])
    if vol.empty:
        return pd.DataFrame(), False
    vol["Territorio"] = vol["aws_region_code"]
    vol["Score_Riesgo"] = (vol["Demanda_Tecnologica"] * vol["Intensidad_Carbono"]) / 1000.0
    vol["Región"] = "AWS"
    return vol, True


def page9_jobs(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    u = bundle["usage"]
    if u.empty:
        return pd.DataFrame(), False
    if "job_type" not in u.columns or "execution_status" not in u.columns:
        return pd.DataFrame(), False
    if "energy_consumed_kwh" not in u.columns:
        return pd.DataFrame(), False
    agg_map: dict[str, Any] = {
        "energy_kwh": ("energy_consumed_kwh", "sum"),
    }
    if "cost_electricity_usd" in u.columns:
        agg_map["cost_usd"] = ("cost_electricity_usd", "sum")
    else:
        agg_map["cost_usd"] = ("energy_consumed_kwh", "sum")
    if "co2_emissions_kg" in u.columns:
        agg_map["emissions_kg"] = ("co2_emissions_kg", "sum")
    else:
        agg_map["emissions_kg"] = ("energy_consumed_kwh", "sum")
    g = u.groupby(["job_type", "execution_status"], as_index=False).agg(**agg_map)
    return g, True


def page10_scale(bundle: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, bool]:
    u, carbon, region, ctry = bundle["usage"], bundle["carbon"], bundle["region"], bundle["country"]
    if u.empty:
        return pd.DataFrame(), False
    if "country_iso" not in u.columns and not region.empty:
        u = u.drop(
            columns=[c for c in u.columns if c.startswith("country_iso_")],
            errors="ignore",
        ).merge(
            region[["aws_region_code", "country_iso"]].drop_duplicates(),
            on="aws_region_code",
            how="left",
            suffixes=("", "_r"),
        )
        if "country_iso_r" in u.columns and "country_iso" not in u.columns:
            u["country_iso"] = u["country_iso_r"]
        u.drop(columns=[c for c in u.columns if c == "country_iso_r"], errors="ignore", inplace=True)
    if "country_iso" not in u.columns:
        return pd.DataFrame(), False
    vol = u.groupby("country_iso", as_index=False).agg(Volumen_kWh=("energy_consumed_kwh", "sum"))
    if carbon.empty or "zone" not in carbon.columns or region.empty:
        return pd.DataFrame(), False
    zm = carbon.groupby("zone")["carbon_intensity_gco2eq_per_kwh"].mean().to_dict()
    int_by_country: dict[str, float] = {}
    for iso in vol["country_iso"].dropna().unique():
        zs = (
            region[region["country_iso"] == iso]["electricity_maps_zone"].dropna().unique()
            if "electricity_maps_zone" in region.columns
            else []
        )
        vals = [zm.get(z) for z in zs if z in zm]
        int_by_country[str(iso)] = float(np.nanmean(vals)) if vals else np.nan
    vol["Intensidad_Carbono"] = vol["country_iso"].astype(str).map(int_by_country)
    vol = vol.dropna(subset=["Intensidad_Carbono"])
    if vol.empty:
        return pd.DataFrame(), False
    name_map = {}
    if not ctry.empty and "iso_code" in ctry.columns and "country_name" in ctry.columns:
        name_map = dict(zip(ctry["iso_code"].astype(str), ctry["country_name"]))
    vol["País"] = vol["country_iso"].map(lambda x: name_map.get(str(x), str(x)))
    tot = vol["Volumen_kWh"].sum()
    vol["Participacion"] = (vol["Volumen_kWh"] / max(tot, 1.0) * 100.0).round(1)
    vol["Región"] = "—"
    return vol, True


def page1_extras(bundle: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """KPIs y series auxiliares para movilidad regional."""
    carbon = bundle["carbon"]
    usage = bundle["usage"]
    out: dict[str, Any] = {"ranking_zones": pd.DataFrame(), "timeline": pd.DataFrame(), "waterfall": None}
    if bundle_has_carbon(bundle):
        cz = carbon.groupby("zone", as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
        cz = cz.sort_values("carbon_intensity_gco2eq_per_kwh")
        mx = cz["carbon_intensity_gco2eq_per_kwh"].max()
        cz["Reducción %"] = (1.0 - cz["carbon_intensity_gco2eq_per_kwh"] / mx) * 100.0 if mx and mx > 0 else 0.0
        cz.rename(columns={"zone": "Región Destino"}, inplace=True)
        cz["Intensidad Carbono"] = cz["carbon_intensity_gco2eq_per_kwh"]
        out["ranking_zones"] = cz
        if "hour" in carbon.columns:
            ts_col = pick_column(carbon, "timestamp", "ts", "datetime", "event_time")
            if ts_col:
                t = pd.to_datetime(carbon[ts_col], errors="coerce")
                carbon = carbon.assign(_m=t.dt.to_period("M").astype(str))
                tl = carbon.groupby(["_m", "zone"], as_index=False)["carbon_intensity_gco2eq_per_kwh"].mean()
                wide = tl.pivot(index="_m", columns="zone", values="carbon_intensity_gco2eq_per_kwh")
                out["timeline"] = wide.reset_index().rename(columns={"_m": "Mes"})
    if bundle_has_usage(bundle) and "co2_emissions_kg" in usage.columns:
        out["total_emissions_tons"] = usage["co2_emissions_kg"].sum() / 1000.0
    return out
