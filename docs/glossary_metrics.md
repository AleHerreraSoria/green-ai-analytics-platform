# Data Standardization & Glossary: Green AI Platform

**Proyecto:** Green AI Analytics Platform (GAIA)
**Ticket:** SRUM-40 (Revisión Integrada v2)
**Responsable:** Alejandro N. Herrera Soria (Data Engineer)

---

## 1. Glosario de Términos Técnicos e Identificadores

Para garantizar la integridad referencial entre los scripts de ingesta y las capas de S3, se definen los siguientes conceptos:

| Término                             | Definición Técnica                                                                            | Mapeo en Repositorio / Código                           |
| :----------------------------------- | :---------------------------------------------------------------------------------------------- | :------------------------------------------------------- |
| **Intensidad de Carbono**      | Emisiones de$CO_2$ equivalente por unidad de electricidad ($gCO_2eq/kWh$).                  | Campo `carbonIntensity` en los JSON de la API.         |
| **Mix Energético**            | Desglose porcentual de fuentes de generación (Solar, Eólica, Fósil, etc.).                   | Objeto `powerProductionBreakdown` en la capa Bronze.   |
| **TDP (Thermal Design Power)** | Potencia máxima que el sistema de enfriamiento del hardware debe disipar.                      | Columna `tdp_watts` en el dataset de hardware.         |
| **Zone ID / Region**           | Identificador geográfico único para nodos de red o regiones de AWS.                           | Variable `ZONE_ID` utilizada en los scripts de Python. |
| **Bucket Suffix**              | Identificador aleatorio único para evitar colisiones de nombres en S3.                         | Generado por el recurso `random_id` en Terraform.      |
| **Idempotencia**               | Propiedad de un pipeline de producir el mismo resultado sin importar cuántas veces se ejecute. | Implementado mediante sobreescritura controlada en S3.   |

---

## 2. Unidades de Medida Estandarizadas

Para evitar errores de cálculo en las transformaciones de la capa Silver a Gold, todo dato debe normalizarse a:

### A. Métricas de Carbono y Medio Ambiente

* **Unidad Base:** $gCO_2eq/kWh$.
* **Normalización:** Cualquier dato recibido en $kg$ o $ton$ debe ser convertido a gramos en la capa Silver para mantener precisión decimal en cálculos de micro-servicios.
* **Frecuencia:** Los datos de la API se consideran "Instantáneos" (Real-time).

### B. Métricas Eléctricas y de Cómputo

* **Consumo de Energía:** $kWh$ (Kilovatios-hora).
* **Potencia Nominal:** $W$ (Vatios). *Nota: Conversión obligatoria para cálculos de costo: $(W \times horas) / 1000$.*
* **Capacidad de Cómputo:** $TFLOPS$ (Teraflops). Utilizado para medir la "Potencia de Fuego" de la instancia `m7i-flex`.

### C. Métricas Financieras y de Infraestructura

* **Divisa:** $USD$ (Dólares estadounidenses).
* **Almacenamiento:** $GB$ (Gigabytes). Unidad base para monitoreo de costos en S3.

---

## 3. Diccionario de Referencia de Datasets (Lineage)

Alineación técnica según los archivos presentes en el repositorio:

1. **Dataset Ingesta API (v-Ingesta):** Provee la intensidad de carbono actual y la señal de "Emisiones Marginales".
2. **Dataset OWID:** Provee el contexto histórico anual para comparativas de tendencia.
3. **Dataset Hardware Specs:** Provee los coeficientes de eficiencia para el cálculo de impacto según el dispositivo de cómputo seleccionado.

---

## 4. Fórmulas de Validación de Datos (Data Quality)

Estas fórmulas deben ser implementadas en los tests de calidad de datos:

* **Emisiones Totales:** $E = [Consumo(kWh) \times Intensidad(gCO_2eq/kWh)] \times PUE$
* **Eficiencia Relativa:** $Ef = \frac{TFLOPS}{Consumo(W)}$

---

*Este documento ha sido contrastado con la rama dev y los despliegues de infraestructura realizados hasta la fecha (Abril 2026).*

*Documento generado para el Proyecto Final DEPT02 - Henry 2026.*
