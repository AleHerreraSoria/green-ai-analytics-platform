# Arquitectura de la Capa Gold — Green AI Analytics Platform

> **Versión:** 1.0 | **Fecha:** 2026-04-21 | **Autor:** Data Engineering Team  
> **Estado:** `ACTIVO` — Refleja la implementación en `gold/silver_to_gold.py` post-refactorización Kimball.

---

## 1. Resumen Ejecutivo

La **capa Gold** es la capa de consumo analítico de la plataforma Green AI. Su propósito es entregar datos limpios, integrados, dimensionalmente estructurados y listos para el consumo directo por dashboards, modelos de análisis y reportes de impacto ambiental de la ONG.

### Contexto de Negocio

La ONG utiliza esta capa para responder **10 preguntas de negocio** agrupadas en tres dominios estratégicos:

| Dominio | Preguntas de Negocio Cubiertos |
|---|---|
| **Movilidad y Ahorro de CO₂** | ¿Cuánto CO₂ se ahorraría migrando cargas de IA a regiones más limpias? ¿Qué regiones AWS tienen la menor huella de carbono? |
| **Eficiencia de Cómputo y TCO** | ¿Qué GPU ofrece el menor costo ambiental por TFLOP? ¿Cuál es el costo total de propiedad (TCO) de cada sesión de entrenamiento? |
| **Limpieza de la Red Eléctrica** | ¿En qué ventanas horarias la red es más verde? ¿Cómo evoluciona la intensidad de carbono por zona a lo largo del año? |
| **Indicadores Macro y Sostenibilidad** | ¿Qué correlación existe entre el PIB per cápita de un país y su participación de energía baja en carbono? ¿Cómo se relacionan las exportaciones TIC con la limpieza de la red? |

### Principios de Diseño

- **Calidad sobre velocidad:** Los datos Gold son el resultado de validaciones en capas Bronze y Silver. Solo registros conformes llegan aquí.
- **Determinismo:** Las Surrogate Keys se generan mediante `md5()` sobre claves naturales normalizadas, lo que garantiza consistencia sin necesidad de tablas de secuencia ni lecturas previas.
- **Idempotencia estricta:** La SparkSession está configurada con `spark.sql.sources.partitionOverwriteMode = dynamic`, asegurando que cada ejecución del pipeline sea segura y repetible.
- **Separación de responsabilidades:** Las tablas de dimensión se cargan con `mode("overwrite")` total; las tablas de hechos se particionan físicamente por tiempo y se sobrescriben de forma granular mediante el modo dinámico.

---

## 2. Arquitectura de Datos — Modelo Kimball (Esquema Constelación)

La capa Gold implementa un **Esquema Constelación** (*Constellation Schema*): múltiples tablas de hechos de distinto grano comparten dimensiones conformadas. Este diseño permite que los analistas combinen hechos de diferentes procesos (sesiones de IA, intensidad de carbono, macrodatos por país) sobre los mismos ejes dimensionales.

### 2.1 Tablas de Dimensión

Las dimensiones son catálogos estáticos o de baja frecuencia de actualización. Se almacenan en Gold **sin partición física** (son tablas pequeñas cuyo coste de escritura total es despreciable) y se sobreescriben en cada ejecución del pipeline.

#### Surrogate Keys — Generación Determinística con MD5

Todas las dimensiones utilizan **Surrogate Keys (SK)** generadas con la función `md5()` de PySpark aplicada sobre la clave natural normalizada (`lower(trim(natural_key))`). Esta estrategia garantiza:

- **Consistencia cross-tabla:** La misma entidad genera el mismo hash en cualquier punto del pipeline, eliminando la necesidad de hacer `lookup joins` contra la tabla de dimensión para obtener la SK.
- **Idempotencia:** Ejecutar el pipeline N veces produce exactamente los mismos hashes.
- **Desacoplamiento:** Los jobs de hechos regeneran las FKs en línea sin leer las tablas de dimensión desde Gold, reduciendo la dependencia de orden de ejecución.

| Tabla | Clave Natural | Surrogate Key | Fuentes Silver |
|---|---|---|---|
| `dim_country` | `iso_alpha3` | `md5(iso_alpha3)` | `owid` ⊕ `reference/world_bank_metadata` |
| `dim_region` | `cloud_provider \| cloud_region` | `md5(cloud_provider\|cloud_region)` | `reference/geo_cloud_mapping` |
| `dim_gpu_model` | `gpu_model` | `md5(gpu_model)` | `mlco2/gpus` |
| `dim_instance_type` | `instance_type \| cloud_region` | `md5(instance_type\|cloud_region)` | `reference/ec2_pricing` |
| `dim_electricity_price` | `country` (petrol prices) | `md5(country)` | `global_petrol_prices` |

#### Descripción de cada dimensión

- **`dim_country`** — Catálogo de países enriquecido. Combina los códigos ISO Alpha-3 de OWID con los metadatos de región geográfica e income group del World Bank. Es la dimensión de referencia para todos los análisis macroenergéticos y de sostenibilidad por país.

- **`dim_region`** — Tabla puente de regiones cloud. Mapea cada región AWS (`cloud_region`) a su zona de Electricity Maps, código ISO del país y coordenadas. Es la dimensión central que une los logs de uso de IA con los datos de intensidad de carbono y precios eléctricos. Sin esta tabla, los JOINs geográficos son imposibles.

- **`dim_gpu_model`** — Catálogo de modelos de GPU extraído de los datos MLCO2. Incluye TDP (Thermal Design Power en vatios) y TFLOPS en precisión FP32 y FP16, las métricas clave para calcular la eficiencia energética y el costo ambiental por operación de cómputo.

- **`dim_instance_type`** — Catálogo de tipos de instancia EC2. Almacena el precio on-demand por hora (`price_usd_per_hour`), que actúa como proxy del costo de cómputo directo (TCO). La granularidad es `(instance_type, cloud_region)` porque los precios varían por región.

- **`dim_electricity_price`** — Precio eléctrico residencial de referencia por país, extraído de Global Petrol Prices. El precio se trata como estático (snapshot de referencia). La resolución de FK hacia `dim_country` se realiza en las tablas de hechos a través de `dim_region`, dado que `global_petrol_prices` no contiene códigos ISO directos.

---

### 2.2 Dimensiones Degeneradas

Por decisión de diseño explícita, los atributos **`job_type`** y **`execution_status`** se mantienen como columnas de tipo `STRING` directamente dentro de `fact_ai_compute_usage`, en lugar de crear tablas `dim_job_type` y `dim_execution_status` separadas.

**Justificación:**

| Criterio | Decisión |
|---|---|
| **Cardinalidad** | `job_type` tiene 3 valores posibles; `execution_status` tiene 2. Crear tablas separadas de 2–3 filas añade complejidad sin beneficio analítico. |
| **Rendimiento** | Eliminar dos JOINs en la consulta más frecuente del dashboard (`fact_ai_compute_usage`) reduce latencia de forma medible. |
| **Convención Kimball** | El modelo Kimball reconoce las dimensiones degeneradas como patrón legítimo para atributos de baja cardinalidad derivados directamente del hecho transaccional. |

Los valores válidos de cada dimensión degenerada están documentados y validados en `silver/validations.py`.

---

### 2.3 Tablas de Hechos

El modelo implementa tres tablas de hechos de granularidad diferente, que conforman el esquema constelación:

| Tabla de Hechos | Grano | Volumen Esperado | Partición Física |
|---|---|---|---|
| `fact_ai_compute_usage` | Una fila por sesión de cómputo de IA | Alto (millones de sesiones) | `year`, `month` |
| `fact_carbon_intensity_hourly` | Una fila por zona × hora | Muy alto (series temporales horarias) | `year`, `month` |
| `fact_country_energy_annual` | Una fila por país × año | Bajo (< 10,000 filas) | `year` |

#### `fact_ai_compute_usage`

Es la tabla de hechos principal del modelo. Registra cada sesión de cómputo de IA con sus métricas de energía, costo y emisiones estimadas. Todas las surrogate keys de las dimensiones se regeneran en línea durante el proceso ETL mediante las mismas fórmulas `md5()` aplicadas en las dimensiones, garantizando la integridad referencial sin requerir JOINs previos contra Gold.

Métricas calculadas en el pipeline:

- `cost_electricity_usd` = `energy_consumed_kwh` × `residential_usd_per_kwh`
- `cost_compute_usd` = `duration_hours` × `price_usd_per_hour`

> [!NOTE]
> El cálculo de métricas usa `F.when(...isNotNull(), A * B).otherwise(None)` en lugar de `coalesce()`. Si alguno de los factores es `NULL` (lookup sin match), la métrica resultante es `NULL` — semántica correcta de negocio que preserva la ausencia de información sin inventar datos.

#### `fact_carbon_intensity_hourly`

Captura la intensidad de carbono horaria por zona eléctrica. Es la base de datos para identificar **ventanas verdes** de cómputo (horas con baja intensidad de carbono) y para los análisis de ahorro por movilidad regional. El pipeline implementa una estrategia de lectura resiliente: intenta los endpoints en orden `history → latest → past` y falla explícitamente si ninguno está disponible.

#### `fact_country_energy_annual`

Consolida los indicadores macroenergéticos y económicos anuales por país mediante un JOIN entre OWID (indicadores energéticos, GHG, GDP) y World Bank ICT Exports. Es la base para los análisis de correlación entre desarrollo económico, mix eléctrico y exportaciones tecnológicas. La FK `country_id` usa el mismo hash `md5(iso_alpha3)` que `dim_country`, garantizando la trazabilidad del modelo estrella.

---

## 3. Resiliencia e Idempotencia — El Motor ETL

La robustez del pipeline Gold se sustenta en tres mecanismos complementarios implementados en **`gold/silver_to_gold.py`**.

### 3.1 Configuración Crítica de Spark: Dynamic Partition Overwrite

La SparkSession se inicializa con la siguiente configuración obligatoria:

```python
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

Esta única directiva cambia radicalmente el comportamiento de las escrituras con `.partitionBy()`:

| Modo | Comportamiento al ejecutar con `mode("overwrite")` |
|---|---|
| **`static`** (default) | Borra **toda** la tabla y reescribe solo las particiones del lote actual. **Destruye el histórico.** |
| **`dynamic`** | Borra y reescribe **únicamente** las particiones presentes en el DataFrame actual. El histórico de otras particiones permanece intacto. |

> [!IMPORTANT]
> Sin `partitionOverwriteMode = dynamic`, una ejecución del pipeline que procese solo datos de `year=2024, month=12` borraría silenciosamente todos los datos de años anteriores. Esta configuración es **no negociable** para garantizar la integridad histórica.

### 3.2 Estrategia de Escritura por Tipo de Tabla

**Dimensiones** — Tablas de catálogo pequeñas y estáticas:

```python
dim.write
   .format("delta")
   .mode("overwrite")           # Overwrite total de la tabla completa
   .option("overwriteSchema", "true")
   .save(f"{GOLD}/dim_country")
```

Las dimensiones se sobreescriben en su totalidad en cada ejecución. Al ser tablas de baja cardinalidad (cientos o miles de filas), el coste de reescritura total es despreciable, y este enfoque elimina cualquier riesgo de duplicados o SCD no controlados.

**Tablas de Hechos** — Tablas de alto volumen con partición temporal:

```python
fact.write
    .format("delta")
    .mode("overwrite")            # Overwrite solo de las particiones del lote
    .option("overwriteSchema", "true")
    .partitionBy("year", "month") # (o "year" para fact_country_energy_annual)
    .save(f"{GOLD}/fact_ai_compute_usage")
```

El `.partitionBy("year", "month")` combinado con `partitionOverwriteMode = dynamic` garantiza que:

1. Un backfill de `2024-01` reescribe únicamente la partición `year=2024/month=1`.
2. Las particiones de `2023` o de otros meses de `2024` no son tocadas.
3. Re-ejecutar el mismo lote produce exactamente el mismo resultado físico en S3 (**idempotencia pura**).

### 3.3 Política de Fallos Escalonada en `main()`

El orquestador implementa una política de fallos en dos fases:

```
Fase 1: Dimensiones
  └─ Si CUALQUIER dimensión falla → sys.exit(1) inmediato.
     Los hechos no pueden ejecutarse sin dimensiones coherentes.

Fase 2: Hechos
  └─ Si UN hecho falla → los demás continúan ejecutándose.
     Al finalizar, si hay errores acumulados → sys.exit(1).
```

Este diseño maximiza la información diagnóstica: un fallo en `fact_carbon_intensity_hourly` no impide que `fact_country_energy_annual` se complete correctamente, facilitando la recuperación parcial y la depuración.

---

## 4. Data Quality Gate — Validaciones Gold

El script **`gold/gold_validations.py`** actúa como **QA Gate obligatorio** al final del pipeline. Es el guardián que garantiza que ningún dato incorrecto llegue al dashboard de la ONG.

### 4.1 Estructura del Módulo

El módulo replica la arquitectura de `silver/validations.py` con dataclasses tipadas:

```python
@dataclass
class GoldCheckResult:
    table: str          # Tabla Gold validada
    check_name: str     # Nombre descriptivo del check
    passed: bool        # True = OK, False = fallo
    detail: str         # Mensaje con conteos concretos
    failing_count: int  # Número de registros problemáticos
    severity: str       # "ERROR" (bloquea pipeline) | "WARN" (alerta, no bloquea)
```

Los checks se acumulan en un `GoldValidationReport` y se imprimen al final en un reporte tabular estructurado por logger.

### 4.2 Las Tres Validaciones Principales

#### Validación 1 — Integridad Referencial (`region_id → dim_region`)

```python
orphans = (
    fact_df
    .filter(F.col("region_id").isNotNull())          # Excluye NULLs válidos
    .join(known_regions, "region_id", "left_anti")   # Registros sin match en dim
)
```

El **`left_anti` join** retiene únicamente las filas de `fact_ai_compute_usage` cuyo `region_id` **no existe** en `dim_region`. Solo se comprueban los `region_id` no nulos: los NULLs son sesiones sin región resuelta (dato faltante legítimo) y se gestionan por separado con un check de tasa de nulos.

Si `orphan_count > 0`, el reporte incluye hasta 10 ejemplos de hashes huérfanos para diagnóstico inmediato.

| Severidad | Umbral | Consecuencia |
|---|---|---|
| **ERROR** | 1 o más huérfanos | Fallo del QA Gate |

#### Validación 2 — Reglas de Negocio (métricas no negativas)

```python
negatives = df.filter(
    F.col("energy_consumed_kwh").isNotNull() &
    (F.col("energy_consumed_kwh") < 0)
).count()
```

Se validan tres columnas críticas de `fact_ai_compute_usage`:

| Columna | Regla | Justificación |
|---|---|---|
| `energy_consumed_kwh` | `>= 0` | No existe consumo energético negativo físicamente |
| `cost_electricity_usd` | `>= 0` | Un costo negativo indicaría un error de signo en la multiplicación |
| `cost_compute_usd` | `>= 0` | Protege contra precios EC2 mal parseados como negativos |

> [!NOTE]
> Los `NULL` en estas columnas **no** se cuentan como violaciones. Un `NULL` indica que el lookup no encontró precio (por ejemplo, una región sin precio EC2 mapeado), lo que es un dato faltante válido —no un valor incorrecto.

#### Validación 3 — Unicidad de `session_id`

```python
total_rows   = df.count()
distinct_ids = df.select("session_id").distinct().count()
duplicates   = total_rows - distinct_ids
```

Esta validación detecta duplicados introducidos por JOINs cruzados erróneos (fan-out). Si un lookup join produce más de un match por `session_id` —por ejemplo, dos filas en `ec2_pricing` para la misma región e instancia— la tabla de hechos tendrá filas duplicadas que inflarían artificialmente todas las métricas agregadas.

| Severidad | Umbral | Consecuencia |
|---|---|---|
| **ERROR** | 1 o más duplicados | Fallo del QA Gate |

### 4.3 Integración en CI/CD y Airflow

El flag **`--fail`** en la invocación CLI hace que el script salga con código de retorno `1` si algún check de severidad `ERROR` falla:

```python
if fail_on_error and not report.all_passed:
    sys.exit(1)
```

Esto permite integrarlo directamente como step de un DAG de Airflow o en un pipeline de CI/CD:

**Airflow (BashOperator):**
```python
validate_gold = BashOperator(
    task_id="validate_gold_quality",
    bash_command="python /opt/airflow/dags/gold/gold_validations.py --fail",
)

# Dependencia: validaciones DESPUÉS del pipeline Gold
build_gold >> validate_gold
```

Si **`gold_validations.py --fail`** retorna código 1, Airflow marcará el task como `FAILED` y detendrá los tasks downstream —por ejemplo, la actualización del dashboard o el envío de reportes a la ONG—, **evitando que datos incorrectos lleguen a producción**.

**GitHub Actions (CI):**
```yaml
- name: Validate Gold Layer Quality
  run: python green-ai-analytics-platform/gold/gold_validations.py --fail
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

---

## 5. Instrucciones de Ejecución

### 5.1 Prerrequisitos

```bash
# Variables de entorno requeridas (o definidas en .env)
export AWS_ACCESS_KEY_ID="<tu-access-key>"
export AWS_SECRET_ACCESS_KEY="<tu-secret-key>"
export S3_SILVER_BUCKET="green-ai-pf-silver-a0e96d06"  # opcional, es el default
export S3_GOLD_BUCKET="green-ai-pf-gold-a0e96d06"      # opcional, es el default
```

> [!IMPORTANT]
> Los JARs de Delta Lake, Hadoop-AWS y AWS SDK se resuelven automáticamente en el primer arranque de Spark via `spark.jars.packages`. Es necesario tener acceso a Maven Central o un proxy interno en el entorno de ejecución.

### 5.2 Ejecutar el Pipeline Gold Completo

```bash
# Desde la raíz del repositorio
python green-ai-analytics-platform/gold/silver_to_gold.py
```

**Salida esperada en el log (resumen):**

```
2026-04-21T19:00:00 [INFO] green-ai.gold — SparkSession iniciada. partitionOverwriteMode=dynamic ✓
2026-04-21T19:00:05 [INFO] green-ai.gold — === Green AI — Silver → Gold | Pipeline Dimensional Kimball ===
2026-04-21T19:00:05 [INFO] green-ai.gold — --- PASO 1/2: Construcción de Dimensiones ---
2026-04-21T19:01:10 [INFO] green-ai.gold — [dim_country]           OK — 195 filas escritas en Gold.
2026-04-21T19:01:15 [INFO] green-ai.gold — [dim_region]            OK — 42 filas escritas en Gold.
2026-04-21T19:01:20 [INFO] green-ai.gold — [dim_gpu_model]         OK — 87 filas escritas en Gold.
2026-04-21T19:01:25 [INFO] green-ai.gold — [dim_instance_type]     OK — 312 filas escritas en Gold.
2026-04-21T19:01:30 [INFO] green-ai.gold — [dim_electricity_price] OK — 150 filas escritas en Gold.
2026-04-21T19:01:30 [INFO] green-ai.gold — ✅ Todas las dimensiones construidas correctamente.
2026-04-21T19:01:30 [INFO] green-ai.gold — --- PASO 2/2: Construcción de Tablas de Hechos ---
2026-04-21T19:05:00 [INFO] green-ai.gold — [fact_ai_compute_usage]        OK — 1,250,000 filas escritas en Gold.
2026-04-21T19:07:30 [INFO] green-ai.gold — [fact_carbon_intensity_hourly] OK — 8,760,000 filas escritas en Gold.
2026-04-21T19:09:00 [INFO] green-ai.gold — [fact_country_energy_annual]   OK — 6,240 filas escritas en Gold.
2026-04-21T19:09:00 [INFO] green-ai.gold — ✅ Pipeline Gold completado exitosamente. Todas las tablas escritas.
```

### 5.3 Ejecutar las Validaciones Gold (modo informativo)

```bash
# Sin --fail: imprime el reporte completo y siempre retorna código 0
python green-ai-analytics-platform/gold/gold_validations.py
```

Útil para diagnóstico manual o exploración durante el desarrollo.

### 5.4 Ejecutar las Validaciones Gold (modo bloqueante para CI/CD)

```bash
# Con --fail: retorna código 1 si algún check ERROR falla
python green-ai-analytics-platform/gold/gold_validations.py --fail
```

**Salida esperada si todas las validaciones pasan:**

```
========================================================================
  VALIDACIÓN CAPA GOLD — Green AI Analytics Platform
========================================================================
  ✅ PASS   [dim_country]           tabla_no_vacía                195 filas encontradas
  ✅ PASS   [dim_country]           pk_not_null(country_id)       0 NULLs de 195 filas
  ✅ PASS   [dim_region]            tabla_no_vacía                42 filas encontradas
  ...
  ✅ PASS   [fact_ai_compute_usage] integridad_ref(region_id → dim_region)  0 huérfanos de 1,250,000
  ✅ PASS   [fact_ai_compute_usage] unique(session_id)            0 duplicados de 1,250,000
  ✅ PASS   [fact_ai_compute_usage] energy_consumed_kwh >= 0      0 valores negativos
  ✅ PASS   [fact_ai_compute_usage] cost_electricity_usd >= 0     0 valores negativos
  ✅ PASS   [fact_ai_compute_usage] cost_compute_usd >= 0         0 valores negativos
  ...
------------------------------------------------------------------------
  Resultado: 18/18 checks pasaron | 0 errores | 0 advertencias
========================================================================
```

**Salida si se detectan huérfanos de integridad referencial:**

```
  ❌ ERROR  [fact_ai_compute_usage] integridad_ref(region_id → dim_region)
            342 registros huérfanos de 1,250,000. Ejemplos: ['a3f1c...', 'b9d2e...']
------------------------------------------------------------------------
  Resultado: 17/18 checks pasaron | 1 errores | 0 advertencias
========================================================================
ERROR - Pipeline abortado: 1 check(s) ERROR fallaron.
```

El proceso termina con `exit code 1`, deteniendo cualquier step downstream en Airflow o CI/CD.

---

*Documento generado por el equipo de Data Engineering — Green AI Analytics Platform.*
*Para modificaciones, consultar el canal `#data-engineering` y actualizar la versión en el encabezado.*

