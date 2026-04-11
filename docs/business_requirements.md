# Business Requirements & Analytics Specification

**Proyecto:** Green AI Analytics Platform (GAIA)
**Versión:** 1.0.0
**Responsable:** Alejandro N. Herrera Soria
**Stakeholder:** ONG Sostenibilidad Digital América

---

## 1. Visión del Proyecto

Desarrollar una arquitectura de datos moderna (Modern Data Stack) capaz de cuantificar y optimizar la huella de carbono generada por procesos de Inteligencia Artificial en América. La plataforma permite la toma de decisiones basada en datos para la migración de cargas de trabajo hacia regiones con matrices energéticas más limpias.

## 2. Indicadores Clave de Desempeño (KPIs)

Para medir el éxito de la plataforma y el impacto en la ONG, se definen los siguientes indicadores:

* **Carbon Intensity Index (CII):** Medición en tiempo real de gCO2eq/kWh por región de cómputo.
* **Energy Efficiency Ratio (EER):** Relación entre la capacidad de cómputo desplegada (TFLOPS) y la emisión de carbono resultante.
* **Data Mobility Savings:** Estimación de toneladas de CO2 ahorradas al preferir una región sobre otra para procesos batch de entrenamiento de modelos.
* **Infrastructure Reliability:** Disponibilidad del entorno productivo (EC2/Docker) y éxito de los pipelines de Airflow.

---

## 3. Preguntas de Negocio Críticas

Estas preguntas han sido validadas contra las fuentes de datos disponibles (**Electricity Maps, Our World in Data, MLCO2 Hardware**) y serán respondidas mediante los dashboards de BI en la capa `Gold`.

### A. Operación en Tiempo Real

1. **¿Qué región de cómputo en América es un 20% más limpia para procesar hoy en comparación con el promedio diario?**
   * *Fuente:* API Electricity Maps.
2. **En este momento, ¿cuál es la zona con mayor porcentaje de energía renovable inyectada en la red?**
   * *Fuente:* API Electricity Maps.

### B. Análisis Estratégico y de Hardware

3. **¿Qué ahorro porcentual de CO2 se obtendría si movemos una carga de procesamiento de 100 TFLOPS de EE. UU. (Virginia) a Uruguay o Chile?**
   * *Fuente:* MLCO2 Hardware Table + API Real-time.
4. **¿Cuál es el hardware (GPU/CPU) más eficiente disponible en el mercado para realizar inferencia con el menor impacto ambiental?**
   * *Fuente:* MLCO2 Hardware Specs.
5. **¿Qué modelos de GPU presentan la mejor relación entre rendimiento (Performance) y emisiones de carbono durante el entrenamiento?**
   * *Fuente:* MLCO2 / Dataset Hardware.

### C. Tendencias e Impacto Socioeconómico

6. **¿Cómo ha evolucionado la mezcla energética (fósil vs. renovable) en los países de América Latina en la última década?**
   * *Fuente:* Our World in Data (Energy Dataset).
7. **¿Existe una correlación entre el crecimiento del PIB digital de un país y su intensidad de carbono eléctrica?**
   * *Fuente:* Our World in Data + World Bank Open Data.

---

## 4. Criterios de Aceptación Técnica (Data Engineering)

Para que el entregable sea considerado de calidad "Enterprise", debe cumplir con:

- **Idempotencia:** Los pipelines de Airflow pueden re-ejecutarse sin duplicar datos ni corromper las capas S3.
- **Trazabilidad (Data Lineage):** Cada dato en la capa Gold debe ser rastreable hasta su origen en la capa Bronze.
- **Escalabilidad:** La infraestructura en AWS (m7i-flex.large) debe soportar el procesamiento de los 3 datasets principales de forma simultánea.
- **Seguridad:** Uso estricto de variables de entorno (`.env`) y gestión de accesos vía IAM para el equipo de ingeniería.

---

*Documento generado para el Proyecto Final DEPT02 - Henry 2026.*
