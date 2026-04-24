# Documento de Trade-offs y Decisiones Técnicas  
**Proyecto:** Green AI – Data Lakehouse  

---

## 1. Introducción

Este documento describe las decisiones técnicas tomadas durante el diseño de la arquitectura del proyecto, junto con sus respectivos trade-offs.

El objetivo es justificar la elección del stack tecnológico en función de:
- Costo
- Escalabilidad
- Facilidad de mantenimiento

---

## 2. Decisión 1: AWS vs GCP

### Alternativas evaluadas:
- AWS
- Google Cloud Platform (GCP)

### Comparación:

| Criterio | AWS | GCP |
|--------|-----|-----|
| Free Tier | Amplio y flexible | Limitado en algunos servicios |
| Integración | Alta compatibilidad entre servicios | Buena, pero menos utilizada en el equipo |
| Curva de aprendizaje | Media | Media |
| Uso en la industria | Muy alto | Alto |

### Decisión:
Se eligió AWS.

### Justificación:
- Mejor aprovechamiento del Free Tier (costo $0)
- Mayor familiaridad del equipo
- Amplia documentación y adopción en la industria

---

## 3. Decisión 2: Spark vs Pandas

### Alternativas evaluadas:
- PySpark
- Pandas

### Comparación:

| Criterio | Spark | Pandas |
|--------|------|--------|
| Escalabilidad | Alta (distribuido) | Baja (memoria local) |
| Volumen de datos | Grandes volúmenes | Datos pequeños/medianos |
| Performance | Alto en grandes datasets | Bueno en datasets pequeños |
| Complejidad | Mayor | Menor |

### Decisión:
Se eligió Spark.

### Justificación:
- Permite escalar a grandes volúmenes de datos
- Preparado para procesamiento distribuido
- Alineado con arquitectura de Data Lake

---

## 4. Decisión 3: S3 vs Base de Datos Relacional (RDS)

### Alternativas evaluadas:
- Amazon S3
- Bases de datos relacionales (ej: RDS)

### Comparación:

| Criterio | S3 | RDS |
|--------|----|-----|
| Tipo de datos | No estructurados / semi-estructurados | Estructurados |
| Escalabilidad | Alta | Media |
| Costo | Bajo (Free Tier) | Mayor |
| Flexibilidad | Alta | Limitada al esquema |

### Decisión:
Se eligió S3.

### Justificación:
- Ideal para arquitectura Data Lake
- Permite almacenar datos en formato raw (Bronze)
- Más económico y flexible

---

## 5. Decisión 4: Airflow vs Cron

### Alternativas evaluadas:
- Apache Airflow
- Cron

### Comparación:

| Criterio | Airflow | Cron |
|--------|--------|------|
| Orquestación | Avanzada (DAGs) | Básica |
| Monitoreo | Integrado | Limitado |
| Escalabilidad | Alta | Baja |
| Facilidad de uso | Media | Alta |

### Decisión:
Se eligió Airflow.

### Justificación:
- Permite modelar pipelines complejos
- Mejor visibilidad y control de procesos
- Estándar en ingeniería de datos

---

## 6. Conclusión

El stack tecnológico seleccionado (AWS, S3, Spark, Airflow) es el más adecuado para este proyecto debido a:

- **Costo:** Permite operar dentro del Free Tier ($0 USD)
- **Escalabilidad:** Soporta crecimiento futuro del volumen de datos
- **Mantenimiento:** Uso de herramientas estándar y ampliamente documentadas

Además, las decisiones tomadas permiten evolucionar el sistema hacia arquitecturas más complejas (como streaming) sin necesidad de rediseñar completamente la solución.
