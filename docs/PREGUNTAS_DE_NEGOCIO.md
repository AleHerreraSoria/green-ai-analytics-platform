# Preguntas de negocio del ELT

Este documento enlaza cada pregunta de negocio con los **datos necesarios**, su **ubicación** (archivo local, API o referencia en `DICCIONARIO_DE_DATOS.md`) y las **operaciones** típicas para responderla en el pipeline (Bronze → Silver → Gold / capa de serving).

La nomenclatura de rutas es relativa a la raíz del repositorio salvo que se indique URL.

---

## 1. Movilidad regional y horas verdes (ahorro de CO₂ al desplazar cómputo)

**Pregunta:** ¿Cuántas toneladas de CO₂ equivalente se evitarían (o qué reducción porcentual de huella) si el entrenamiento de IA se ejecuta en otra región (p. ej. México hacia Brasil o Canadá) priorizando franjas de baja intensidad de carbono?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Energía del trabajo de entrenamiento (kWh); intensidad de carbono del origen y del destino en la ventana temporal considerada (gCO₂eq/kWh); opcionalmente mix o granularidad horaria para definir “horas verdes”. |
| **Fuentes** | **Logs sintéticos:** campos `region`, `gpu_model`, `duration_hours`, `gpu_utilization`, `energy_consumed_kwh`, `job_type`, `timestamp` (ver sección 4 de `DICCIONARIO_DE_DATOS.md`). **Catálogo GPU:** `MLCO2/gpus.csv` (TDP si recalculas energía). **Intensidad en tiempo (origen/destino):** API **Electricity Maps** — endpoints de intensidad *Latest* / *Past* / *History* (campos `zone`, `carbonIntensity`, `datetime`); documentado en sección 2 de `DICCIONARIO_DE_DATOS.md` y `datos-api.md`. **Alternativa anual o por zona:** `MLCO2/impact.csv` (`impact` en gCO₂eq/kWh por `region`) o `MLCO2/2021-10-27yearly_averages.csv`; **serie histórica país:** `OWID/owid-energy-data.csv` — columna `carbon_intensity_elec` (no sustituye la granularidad horaria). **Mapeo geográfico:** `bronze/reference/geo_cloud_to_country_and_zones.csv` (región cloud ↔ zona EM ↔ ISO); catálogo de zonas de Electricity Maps (sección 2.3 del diccionario). |
| **Operaciones** | Filtrar sesiones `job_type = Training` (o la definición acordada). Unir logs con factor de intensidad por `zone` y ventana de tiempo. Para “horas verdes”: filtrar/agrupar por hora donde `carbonIntensity` ≤ umbral o percentil; calcular \(\Delta\) emisiones \(=\) kWh × (CI\_origen − CI\_destino) por intervalo; sumar y convertir g → **tCO₂eq**. Para **% reducción:** comparar huella en origen vs escenario destino sobre el mismo kWh. Normalizar zonas horarias si comparas países distintos. |

---

## 2. Elección de GPU para la ONG (costo eléctrico + huella de carbono)

**Pregunta:** ¿Qué modelo de GPU es más eficiente en términos económicos y ambientales para la organización, combinando precio de la electricidad, consumo energético del chip y factor de emisión de la región?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Especificaciones por GPU (TDP, opcionalmente TFLOPS/W); precio de electricidad (USD/kWh) por país; factor de emisión (gCO₂eq/kWh) por región o país; región de cómputo asociada al escenario. **Opcional (TCO cómputo):** tarifa horaria instancia (`bronze/reference/aws_ec2_on_demand_usd_per_hour.csv`) para coste ≈ horas × USD/h además del coste eléctrico. |
| **Fuentes** | `MLCO2/gpus.csv` — `name`, `tdp_watts`, `GFLOPS32/W`, `GFLOPS16/W`. `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` — tarifas residencial/negocio (renombrar columnas en pipeline). `MLCO2/impact.csv` — `region`, `impact`, `offsetRatio`. API Electricity Maps o `OWID/owid-energy-data.csv` (`carbon_intensity_elec`) para consistencia país-año. **Mapeo geográfico:** `bronze/reference/geo_cloud_to_country_and_zones.csv` (región cloud ↔ ISO ↔ nombre país precios ↔ zona EM); ver sección 7 de `DICCIONARIO_DE_DATOS.md`. **Precio instancia (proxy TCO):** `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` (sección 8 del diccionario); join por `region` + `instance_type` de logs. |
| **Operaciones** | Para cada GPU y país/región: **costo eléctrico** por hora \(\approx\) (TDP/1000) × USD/kWh (ajustar por utilización si el KPI es por sesión real). **Costo cómputo (proxy TCO):** por sesión \(\approx\) `duration_hours` × `price_usd_per_hour` del catálogo EC2 (misma `region` e `instance_type` que el log). Estimar emisiones por hora \(\approx\) (TDP/1000) × gCO₂eq/kWh. Opcional: dividir por TFLOPS para **costo/TFLOP** y **gCO₂eq/TFLOP**. Normalizar moneda y año. Puede ponderarse con pesos ONG (ej. 60 % costo / 40 % carbono); aclarar si “costo” mezcla electricidad, instancia o ambos. |

---

## 3. Costo de carbono por TFLOP y variación por país o zona eléctrica

**Pregunta:** ¿Cuál es el costo de carbono por TFLOP entre GPUs de referencia (p. ej. H100 vs A100) y cómo cambia según el país o la zona eléctrica?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | TFLOPS (FP16/FP32 según definición del KPI) y potencia (TDP) por chip; factor de emisión gCO₂eq/kWh por zona/país. |
| **Fuentes** | `MLCO2/gpus.csv` — `name`, `TFLOPS32`, ` TFLOPS16`, `tdp_watts`. `MLCO2/impact.csv` o Electricity Maps / OWID como en la pregunta 2. Verificar que los modelos comparados existan en `gpus.csv`. |
| **Operaciones** | gCO₂eq por TFLOP-h o por unidad de trabajo: \(\frac{\text{TDP}/1000 \times \text{CI (g/kWh)}}{\text{TFLOPS}}\) con CI alineada a la misma región. Comparar pares de GPUs filtrando por `name`. Tabla cruzada **GPU × país/región** (join dimensión geográfica). |

---

## 4. Sector digital (exportaciones TIC) y limpieza de la red eléctrica

**Pregunta:** ¿Existe correlación entre la evolución de las exportaciones de servicios TIC (proxy del dinamismo del sector digital) y la “limpieza” de la red (intensidad de carbono o baja carbono)?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Serie anual de exportaciones de servicios TIC (USD); serie anual por país de intensidad de carbono de la electricidad y/o participación de bajo carbono en generación; identificador común de país (ISO3). |
| **Fuentes** | `World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` — columnas `Country Code`, años `1960`–`2025` (indicador `BX.GSR.CCIS.CD`). `OWID/owid-energy-data.csv` — `iso_code`, `year`, `carbon_intensity_elec`, `low_carbon_share_elec`, `renewables_share_elec`, etc. Metadatos país: `World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv` para regiones. |
| **Operaciones** | Join por `iso_code` / `Country Code` y `year`. Filtrar países y ventana temporal con datos completos. Calcular tasas de crecimiento (YoY) o log-diferencias de exportaciones TIC; alinear con `carbon_intensity_elec` o shares. Correlación de Pearson/Spearman, regresión simple o panel según profundidad. Documentar que el indicador es **exportaciones TIC**, no PIB del sector tecnológico. |

---

## 5. PIB per cápita y electricidad baja en carbono (América)

**Pregunta:** ¿Existe correlación entre el PIB per cápita de los países de América y la capacidad de su red para ofrecer electricidad con baja intensidad de carbono?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | PIB total y población (o PIB per cápita derivado); métrica de red: intensidad de carbono de la electricidad y/o share renovables/bajo carbono; lista de países “América” (filtro geográfico). |
| **Fuentes** | `OWID/owid-energy-data.csv` — `gdp`, `population`, `carbon_intensity_elec`, `low_carbon_share_elec`, `iso_code`, `country`, `year`. |
| **Operaciones** | Calcular PIB per cápita \(=\) `gdp` / `population` (unidades según codebook OWID). Filtrar entidades país (excluir agregados si el CSV los mezcla). Restringir a América mediante lista ISO o join con metadata BM. Un año fijo o panel multi-año. Correlación o scatter con bandas de confianza; opcionalmente controlar por ingreso vía metadata BM. |

---

## 6. Escenario +20% adopción de IA en Latam (demanda eléctrica y emisiones)

**Pregunta:** Ante un incremento del 20% en la adopción/uso de servicios de IA en América Latina, ¿cuál sería el incremento proyectado de demanda eléctrica y de emisiones asociadas al cómputo, desglosado por país?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Línea base de consumo energético atribuible a cargas de IA (kWh) por país o región; supuesto de shock (+20%) sobre esa línea base; intensidad media de carbono por país (gCO₂eq/kWh) para pasar energía → emisiones; opcionalmente demanda eléctrica total del país para contextualizar (% sobre total). |
| **Fuentes** | **Línea base (proxy):** agregación de `energy_consumed_kwh` en logs por `region` → mapeo región → país vía `bronze/reference/geo_cloud_to_country_and_zones.csv`. **Demanda eléctrica total:** `OWID/owid-energy-data.csv` — `electricity_demand` o `electricity_generation`. **Intensidad:** `carbon_intensity_elec` u OWID `greenhouse_gas_emissions` con cuidado de unidades. **Lista Latam:** filtro por países. |
| **Operaciones** | Definir explícitamente el **supuesto**: el +20% aplica al volumen de kWh de IA agregado en logs (o a un subconjunto). Escenario: kWh\_nuevo = kWh\_base × 1.2; emisiones\_delta = kWh\_delta × CI. Desagregar por país vía tabla puente región–país. Reportar como **escenario**, no como pronóstico macro oficial. |

---

## 7. Ventanas operativas de cómputo verde (franjas horarias)

**Pregunta:** ¿En qué franjas horarias la intensidad de carbono cae lo suficiente como para justificar aplazar o concentrar cargas masivas, según la telemetría por zona?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Serie horaria (o subdiaria) de `carbonIntensity` por `zone`; timestamps en UTC; opcional umbral o percentil definido por negocio. |
| **Fuentes** | API **Electricity Maps** — endpoint *History* (lista de registros con `zone`, `carbonIntensity`, `datetime`); estructura en sección 2.1 de `DICCIONARIO_DE_DATOS.md`. Catálogo **Zones** para nombres y país. |
| **Operaciones** | Ingesta Bronze JSON → Silver tabular (`explode` si aplica). Parseo ISO 8601; conversión a hora local del operador si el negocio lo pide. Por zona: rolling mean, percentiles, o flag “hora verde” si CI < umbral. Join opcional con logs para marcar sesiones que podrían haberse desplazado. |

---

## 8. Puntos ciegos de sostenibilidad (alta demanda tecnológica + red sucia)

**Pregunta:** ¿Qué territorios combinan un proxy alto de demanda tecnológica con una matriz eléctrica muy intensiva en carbono?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Proxy de “demanda tecnológica”: volumen de kWh de logs por región/país, o exportaciones TIC (nivel o crecimiento); métrica de “red sucia”: `carbon_intensity_elec` alto o `fossil_share_elec` alto. |
| **Fuentes** | Logs: agregados por `region` y `energy_consumed_kwh`. `World_Bank_Group/API_BX.GSR.CCIS.CD_...` (último año disponible o promedio móvil). `OWID/owid-energy-data.csv` — mismas columnas de intensidad y fósiles. |
| **Operaciones** | Normalizar indicadores (z-score o percentiles) para comparar países. Scatter o matriz 2×2 (alto/bajo demanda tech × alto/bajo CI). Cruce país: join BM + OWID; para logs, agregar por región y mapear a país. |

---

## 9. Comparación por tipo de carga y por resultado de ejecución

**Pregunta:** ¿Cómo difiere el impacto (energía, costo y emisiones) entre Training, Fine-tuning e Inference, y entre ejecuciones exitosas vs fallidas?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Clasificación de carga (`job_type`); estado (`execution_status`); energía (`energy_consumed_kwh`); región para CI y precio kWh. |
| **Fuentes** | Sección 4 de `DICCIONARIO_DE_DATOS.md` — `job_type`, `execution_status`, `energy_consumed_kwh`, `region`. `MLCO2/impact.csv`; `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` vía país. |
| **Operaciones** | GROUP BY `job_type`, `execution_status`: SUM/AVG de kWh; costo = kWh × USD/kWh; emisiones = kWh × gCO₂eq/kWh. Ratios y pruebas de diferencia de medias si aplica. Tratar `Failed` como costo/emisión evitable o desperdicio según narrativa ONG. |

---

## 10. Escala de cómputo vs sostenibilidad (proxy de volumen por región)

**Pregunta:** ¿Los países con mayor volumen de actividad de cómputo (proxy en logs/región) tienden a menor intensidad de carbono por kWh, o hay desacoples entre volumen y limpieza de la red?

| Aspecto | Detalle |
|--------|---------|
| **Datos requeridos** | Agregado de kWh o número de sesiones por región/país (proxy escala); intensidad de carbono eléctrica por mismo país/año. |
| **Fuentes** | Logs — agregación por `region`. `OWID/owid-energy-data.csv` — `carbon_intensity_elec`, `iso_code`, `year`. Mapeo **cloud region → país** (`bronze/reference/geo_cloud_to_country_and_zones.csv`; curación adicional en Gold si aplica). Opcional: `MLCO2/impact.csv` por `region` para alinear con códigos de proveedor. |
| **Operaciones** | Sumar kWh por país (vía mapeo). Join con OWID en el año de referencia. Scatter: volumen vs CI; identificar outliers (alto volumen + alta CI). **Nota:** no hay en el repositorio un inventario oficial de data centers; el volumen de logs es **proxy operativo**, no densidad física de DC. |

---

## Referencia cruzada rápida de artefactos

| Artefacto | Uso principal en estas preguntas |
|-----------|-----------------------------------|
| `DICCIONARIO_DE_DATOS.md` | Definición de columnas, unidades y rutas de todos los datasets locales. |
| `datos-api.md` | Contrato de campos Electricity Maps (Silver). |
| `OWID/owid-energy-data.csv` | Macroeconomía, población, demanda/generación eléctrica, intensidad y mix agregados por país-año. |
| API Electricity Maps | Intensidad (y mix) en tiempo casi real u histórico por `zone`. |
| `MLCO2/*.csv` | Hardware, factores por región de cloud, promedios anuales por zona. |
| `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` | Precio USD/kWh para KPIs económicos. |
| `World_Bank_Group/API_BX.GSR.CCIS.CD_...` | Exportaciones de servicios TIC. |
| `bronze/reference/geo_cloud_to_country_and_zones.csv` | Mapeo región AWS ↔ ISO ↔ país (precios) ↔ zona Electricity Maps. |
| `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv` | Precio horario On-Demand EC2 (Linux) por región y tipo de instancia MLCO2. |
| Logs sintéticos (especificación en diccionario §4) | Sesiones de IA: energía, GPU, región, tipo de job, estado. |

---

## Supuestos transversales (documentar en el proyecto)

- Los **logs sintéticos** son una simulación: los absolutos no sustituyen estadísticas oficiales de consumo de IA por país.
- **Región cloud ↔ país ↔ zona Electricity Maps** se materializa en Bronze como **`bronze/reference/geo_cloud_to_country_and_zones.csv`** (dimensión de referencia versionada); Silver/Gold consumen este archivo o su copia en `s3://.../reference/`.
- Unidades: unificar **gCO₂eq** vs **tCO₂eq**; **kWh** frente a **MWh** en visualizaciones.
