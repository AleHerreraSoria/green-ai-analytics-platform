# Propuesta de Ingesta Continua (Streaming)  
**Proyecto:** Green AI – Data Lakehouse  

---

## 1. Introducción

Actualmente, el pipeline de datos está diseñado bajo un enfoque Batch (procesamiento por lotes).  

Este documento describe una estrategia teórica para evolucionar hacia un modelo de ingesta continua (streaming), en caso de que la organización requiera procesar grandes volúmenes de datos en tiempo real (por ejemplo: 1,000 logs por segundo).

---

## 2. Escenario: Alta tasa de eventos

**¿Qué pasa si mañana llegan 1,000 logs por segundo?**

El sistema actual basado en batch presentaría las siguientes limitaciones:

- Alta latencia (los datos no se procesan inmediatamente)
- Acumulación de archivos en la capa Bronze
- Posible saturación de procesos batch
- Falta de procesamiento en tiempo real

👉 Se requiere una arquitectura orientada a streaming.

---

## 3. Herramientas para Streaming

### 🟢 Apache Kafka (Alternativa recomendada)

Sistema distribuido de mensajería para streaming de datos en tiempo real.

**Ventajas:**
- Alta escalabilidad
- Manejo eficiente de grandes volúmenes de eventos
- Baja latencia
- Permite múltiples consumidores (Spark, otros servicios)

---

### 🟡 Amazon S3 + Eventos (Alternativa simple)

- Los datos se almacenan en S3
- Se generan eventos al subir archivos
- Se activan procesos automáticamente

**Limitaciones:**
- No es streaming puro
- Mayor latencia que Kafka

---

### 🔵 Apache Spark Structured Streaming

- Permite procesar datos en tiempo real
- Compatible con Kafka
- Mantiene coherencia con el stack actual (PySpark)

---

## 4. Flujo de Procesamiento (Streaming)

### Flujo lógico:

1. Los logs son generados por la fuente
2. Se envían a Kafka
3. Spark Streaming consume los datos en tiempo real
4. Los datos se almacenan en:
   - Bronze (raw)
   - Silver (procesados)
   - Gold (agregados)
5. Airflow se utiliza para tareas de control y monitoreo

---

## 5. Arquitectura (Descripción)

- Fuente de datos → Kafka  
- Kafka → Spark Streaming  
- Spark → S3 (Bronze, Silver, Gold)  
- Airflow → Orquestación complementaria  

---

## 6. Posibles Cuellos de Botella

### ⚠️ 1. Saturación de Kafka
- Alta cantidad de eventos puede generar retrasos

**Mitigación:**
- Aumentar particiones
- Escalar brokers

---

### ⚠️ 2. Procesamiento en Spark
- Si Spark no procesa al ritmo de llegada → backlog

**Mitigación:**
- Escalar recursos
- Optimizar jobs

---

### ⚠️ 3. Escritura en S3
- Exceso de archivos pequeños

**Mitigación:**
- Uso de micro-batching
- Formato Parquet optimizado

---

### ⚠️ 4. Complejidad operativa
- Kafka requiere mayor configuración

**Mitigación:**
- Uso de configuraciones básicas
- Documentación clara

---

## 7. Comparación: Batch vs Streaming

| Característica | Batch | Streaming |
|--------------|------|----------|
| Latencia | Alta | Baja |
| Complejidad | Baja | Alta |
| Escalabilidad | Media | Alta |
| Costo | Bajo | Medio |

---

## 8. Conclusión

El enfoque streaming permitiría escalar el sistema y procesar datos en tiempo real.  

Sin embargo, implica mayor complejidad, por lo que se recomienda mantener el enfoque batch actual y evolucionar solo si el negocio lo requiere.