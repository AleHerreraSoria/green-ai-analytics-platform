## Implementación de Calidad de Datos en Capa Gold

En esta etapa se implementaron validaciones finales sobre la tabla `fact_ai_jobs` con el objetivo de asegurar la calidad, coherencia y confiabilidad de los datos antes de su consumo en el dashboard de la ONG.

Las validaciones se realizaron mediante consultas en Athena, permitiendo detectar inconsistencias en métricas clave y asegurar integridad entre datasets.

---

### 🔹 Validación de integridad referencial

Se verificó que las claves utilizadas en la tabla de hechos tengan correspondencia con sus respectivas dimensiones.

**Chequeos implementados:**

```sql
-- Regiones sin match (debería dar 0)
SELECT COUNT(*)
FROM fact_ai_jobs
WHERE region IS NULL;

-- Instance types sin precio asociado (debería dar 0)
SELECT COUNT(*)
FROM fact_ai_jobs
WHERE price_usd_per_hour IS NULL;

-- Países sin datos de emisiones (debería dar 0)
SELECT COUNT(*)
FROM fact_ai_jobs
WHERE carbon_intensity_avg IS NULL;

Resultado esperado:

0 registros sin correspondencia
Relaciones consistentes entre hechos y dimensiones
🔹 Reglas de negocio

Se definieron reglas para validar coherencia en métricas calculadas.

Reglas aplicadas:

Los costos no pueden ser negativos
Las emisiones no pueden ser negativas
Las métricas críticas no deben ser nulas
-- Validación de valores negativos (debería dar 0 filas)
SELECT *
FROM fact_ai_jobs
WHERE cost_energy < 0
   OR cost_compute < 0
   OR emissions_co2 < 0;
-- Validación de nulos en métricas críticas
SELECT 
  COUNT(*) total,
  COUNT(cost_energy) cost_energy_ok,
  COUNT(cost_compute) cost_compute_ok,
  COUNT(emissions_co2) emissions_ok
FROM fact_ai_jobs;
🔹 Validación de duplicados

Se verificó la unicidad de la clave principal (session_id).

SELECT 
  COUNT(*) total,
  COUNT(DISTINCT session_id) unicos
FROM fact_ai_jobs;

Resultado esperado:

total = unicos (sin duplicados)
🔹 Validación de coherencia de métricas

Se evaluó la relación lógica entre consumo energético y emisiones.

-- Detectar inconsistencias entre consumo y emisiones
SELECT *
FROM fact_ai_jobs
WHERE energy_consumed_kwh > 0
  AND emissions_co2 = 0;
🔹 Logging y alertas

Las validaciones permiten detectar automáticamente:

métricas fuera de rango
datos faltantes
errores en joins

En un escenario productivo, estos checks pueden integrarse en el pipeline para generar alertas automáticas (logs o monitoreo).