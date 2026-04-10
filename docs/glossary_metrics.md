
# Data Standardization & Glossary: Green AI Platform

**Proyecto:** Green AI Analytics Platform (GAIA)
**Ticket:** SRUM-40 (Revisión Final Integrada)
**Responsable:** Alejandro N. Herrera Soria (Data Engineer)

---

## 1. Glosario de Términos Técnicos

| Término                             | Definición Técnica                                                           | Relevancia en el Proyecto                                     |
| :----------------------------------- | :----------------------------------------------------------------------------- | :------------------------------------------------------------ |
| **Intensidad de Carbono**      | Emisiones de$CO_2$ equivalente por unidad de electricidad ($gCO_2eq/kWh$). | Métrica core de la API Electricity Maps.                     |
| **Mix Energético**            | Proporción de fuentes (Solar, Eólica, Fósil) en la red.                     | Determina la calidad ambiental de la región.                 |
| **TDP (Thermal Design Power)** | Máxima potencia que el hardware debe disipar.                                 | Proxy para el consumo energético del servidor.               |
| **Idempotencia**               | Propiedad de un proceso de dar el mismo resultado tras múltiples ejecuciones. | Vital para evitar duplicados en S3 durante fallos de Airflow. |

---

## 2. El Factor PUE (Power Usage Effectiveness)

**¿Qué es?** Es la métrica que mide la eficiencia energética de un Centro de Datos. Indica cuánta energía se desperdicia en enfriamiento y mantenimiento vs. cuánta llega realmente al hardware.

$$
PUE = \frac{\text{Energía Total del Data Center}}{\text{Energía consumida por el Servidor}}
$$

### Implementación en GAIA:

* **Origen de la métrica:** Es una **constante de investigación** obtenida de los reportes de sostenibilidad de los proveedores de nube.
* **Valor Estándar:** Usaremos **1.2** para nuestra infraestructura en **AWS**, y **1.8** como valor de referencia para "Servidores Propios/Tradicionales".
* **Uso en el Proyecto:** Se aplicará como un **factor multiplicador** en la capa Gold. Sin el PUE, estaríamos subestimando la huella de carbono real de la ONG en un 20-40%.

---

## 3. Unidades de Medida Estandarizadas

Todo script de procesamiento debe normalizar los datos a estas unidades:

### A. Carbono y Medio Ambiente

* **Unidad Base:** $gCO_2eq/kWh$.
* **Regla de Oro:** Los cálculos internos siempre se hacen en **gramos** para mantener la precisión decimal. La conversión a toneladas es exclusiva para el dashboard.

### B. Electricidad y Hardware

* **Consumo:** $kWh$ (Kilovatios-hora).
* **Potencia:** $W$ (Vatios). *Conversión: $(W \times horas) / 1000$.*
* **Rendimiento:** $TFLOPS$ (Teraflops).

### C. Finanzas e Infraestructura

* **Moneda:** $USD$ (Dólares).
* **Almacenamiento:** $GB$ (Gigabytes).

---

## 4. Protocolo de Cálculo (Capa Gold)

Para asegurar que todos reportemos los mismos números, la fórmula oficial del proyecto es:

$$
Emisiones\_Totales = [Consumo\_Hardware(kWh) \times PUE] \times Intensidad\_Carbono(gCO_2eq/kWh)
$$

---

*Este glosario unifica el lenguaje técnico entre Infraestructura, Ingeniería de Datos y Negocio.*

*Documento generado para el Proyecto Final DEPT02 - Henry 2026.*
