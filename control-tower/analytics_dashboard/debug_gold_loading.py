"""
Script de diagnóstico para validar carga de datos desde Gold.
Ejecutar desde: cd control-tower/analytics_dashboard && python debug_gold_loading.py
"""
import sys
import os
import logging
from pathlib import Path
import pandas as pd

# Configurar logging para ver salida clara
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    print("=" * 70)
    print("DIAGNÓSTICO: Carga de datos desde Gold (S3)")
    print("=" * 70)
    print()
    
    # Agregar el directorio actual (analytics_dashboard) al path
    script_dir = Path(__file__).resolve().parent
    analytics_dashboard_dir = script_dir.parent / "analytics_dashboard"
    
    # Asegurar que el directorio src sea accesible
    if str(analytics_dashboard_dir) not in sys.path:
        sys.path.insert(0, str(analytics_dashboard_dir))
    
    # Importar después de ajustar path
    try:
        # Cambiar al directorio analytics_dashboard para que los imports relative funcionen
        original_cwd = os.getcwd()
        os.chdir(analytics_dashboard_dir)
        
        from src.gold_analytics import load_gold_bundle
    except ImportError as e:
        print(f"ERROR: No se pudo importar 'load_gold_bundle' desde 'src.gold_analytics'")
        print(f"  {e}")
        print()
        print("Asegúrate de ejecutar desde: control-tower/analytics_dashboard/")
        return 1
    
    # Limpiar caché de Streamlit si está disponible
    try:
        import streamlit as st
        st.cache_data.clear()
        logger.info("Caché de Streamlit limpiada")
    except Exception:
        pass
    
    # Cargar el bundle
    logger.info("Cargando bundle Gold...")
    print()
    
    bundle = load_gold_bundle()
    
    # Imprimir resultados por cada tabla lógica
    tables = ["usage", "carbon", "country_energy", "country", "region", "gpu"]
    
    for table_name in tables:
        df = bundle.get(table_name, None)
        
        print("-" * 70)
        print(f"TABLA: {table_name}")
        print("-" * 70)
        
        if df is None or df.empty:
            print(f"  ❌ VACÍO o None")
            print(f"  Filas: 0")
            print(f"  Columnas: []")
        else:
            print(f"  ✓ FILAS: {len(df)}")
            print(f"  COLUMNAS: {list(df.columns)}")
            print()
            print("  Primeras 3 filas:")
            # Mostrar solo las primeras 3 filas de forma legible
            print(df.head(3).to_string(index=True).replace("\n", "\n  "))
        
        print()
    
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    
    # Verificar qué tablas tienen datos
    loaded = [t for t in tables if not bundle.get(t, pd.DataFrame()).empty]
    empty = [t for t in tables if bundle.get(t, pd.DataFrame()).empty]
    
    print(f"Tablas con datos: {len(loaded)}/{len(tables)}")
    for t in loaded:
        print(f"  ✓ {t}")
    
    if empty:
        print(f"\nTablas vacías: {len(empty)}/{len(tables)}")
        for t in empty:
            print(f"  ✗ {t}")
    
    # Indicador de si está usando datos reales
    if loaded:
        print()
        print("✅ Se están leyendo datos REALES desde Gold")
    else:
        print()
        print("⚠️ NO se leen datos de Gold - Revisar:")
        print("  1. Credenciales AWS configuradas en .env")
        print("  2. Bucket S3 accesible")
        print("  3. Tablas Delta/Parquet en el bucket")
    
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())