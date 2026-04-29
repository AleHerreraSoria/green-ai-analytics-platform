"""Script de verificación de normalizaciones."""
import sys
sys.path.insert(0, 'src')

from gold_analytics import load_gold_bundle, page4_macro_scatter, page5_americas

print("=== Verificando normalizaciones ===\n")

# Cargar bundle
bundle = load_gold_bundle()

# Verificar country_energy
print("country_energy columnas:", list(bundle['country_energy'].columns))
print()

# Verificar page4
sd, ok4, note = page4_macro_scatter(bundle)
print(f"page4_macro_scatter: ok={ok4}, filas={len(sd)}")
if not sd.empty:
    print(sd[['País', 'Exportaciones_TIC_MM', 'Intensidad_Carbono']].head(10).to_string())
print()

# Verificar page5
am, ok5 = page5_americas(bundle)
print(f"page5_americas: ok={ok5}, filas={len(am)}")
if not am.empty:
    print(am[['País', 'PIB_per_capita', 'Intensidad_Carbono']].head(10).to_string())