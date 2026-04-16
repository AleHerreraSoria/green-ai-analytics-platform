# 🌿 Green AI — Construcción de la Capa Silver Lakehouse

Bienvenido a la bitácora técnica y documentación principal del pipeline de datos para el proyecto **Green AI**. Este documento resume la evolución, arquitectura y desafíos superados durante la migración de nuestra infraestructura de procesamiento de datos crudos (Bronze) hacia un entorno certificado, robusto y tolerante a fallos (Silver).

---

## 1. Fase de Análisis: El Punto de Partida 🧭

Todo nuestro flujo de datos comenzó con un ecosistema inicial de archivos Parquet y JSON crudos alojados en AWS S3. Para sentar las bases de la Capa Silver, el primer paso fue someter nuestra arquitectura a una ingeniería inversa metodológica. 

Al cruzar el `DICCIONARIO_DE_DATOS.md` original con nuestros requerimientos en `PREGUNTAS_DE_NEGOCIO.md`, identificamos proactivamente tres "gaps" estructurales críticos que hubiesen generado deuda técnica y bloqueado el análisis en capas posteriores (Gold):

1. **Falta de un Catálogo Maestro de Geografías:** Observamos un desacople entre `region` en los logs cloud (ej. `aws-us-east-1`), `zone` en Electricity Maps y los códigos `ISO-3166` macroeconómicos. Evitamos el anti-patrón de aplicar hardcoding en Silver diseñando una tabla puente en Bronze para mapear *Cloud Region -> Grid Zone -> ISO Country Code*.
2. **Ausencia de Precios Cloud Reales (Impacto TCO):** Para evaluar certeramente los costos de infraestructura de la ONG, el precio de red industrial era insuficiente. Abordamos la adición de un catálogo extra con los costos en USD/hora de instancias (EC2, GCP, etc.) permitiendo un cálculo genuino de _Total Cost of Ownership_.
3. **Granularidad Temporal Financiera Limitada:** La tabla de precios presuponía un promedio anual estático, imposibilitando procciones temporales intrincadas (estacionalidades, variaciones intradiarias). Como solución arquitectónica, formalizamos esta limitación operativa estática como un riesgo asumido en el diseño actual para las estimaciones futuras.

El diccionario de datos y los catálogos base fueron inmediatamente unificados, solidificando un contrato de esquema (`schemas.py`) listo y compatible con Spark.

---

## 2. Desarrollo Core: ETL, Calidad y Self-Healing 🛠️

El procesamiento transaccional debía ser blindado. Dividimos la arquitectura en dominios modulares para acoplar validaciones exhaustivas sobre la metadata inyectada:

*   **Modularización del Código:** Diseñamos el pipeline separando responsabilidades: `bronze_to_silver.py` (ETL core), `writer.py` (Lógica de almacenamiento centralizado), `validations.py` (QA Data Contracts) y `audit.py` (Observabilidad inter-capas).
*   **Implementación QA y Limpieza Sensible:** En un ecosistema distribuido, la aserción de calidad es crítica. Desarrollamos filtros de descarte silencioso; un hito importante fue aislar el ruido de logs simulados (`usage_logs`), que permitió expurgar limpiamente más de **500 filas sucias** (un error Tipo I estadístico calculado).
*   **Self-Healing:** Nuestro pipeline de escritura es capaz de registrar anomalías o fallos estructurales en una métrica apartada (`_log_dropped`) sin que el procesamiento lineal se detenga, permitiendo observar métricas de impacto de data decay a futuro.

---

## 3. Etapa de Pruebas en Local (CI Testing) 🧪

Antes del gran salto a las rutas S3, formalizamos un entorno aislado local en memoria, utilizando la carpeta `tests/` y apalancado mediante scripts nativos dentro del directorio `scripts/`. Esta etapa garantizó la precisión antes de inyectar al Lakehouse.

*   **Mocking Local y Stress Testing:** Creamos el script `scripts/generate_mock_data.py`, el cual utiliza la librería `Faker` para forjar un entorno sintético simulado. Este generador inyecta hasta ~5,000 registros sintéticos en directorios espejo locales de `bronze/`, ensuciando de manera calculada un 3% de la integridad total (timestamps corruptos, NaN lógicos, valores fuera del rango esperado de `[0, 1000]`).
*   **Desacople Absoluto del End-point:** Todas las variables de ruta (como `BRONZE_BUCKET` y `SILVER_BUCKET`) fueron abstraídas. Así, el script orquestador `run_local_test.py` apuntaba los paths a nuestro generador local simulando un volumen de ingesta real y resguardando al Data Swamp de contaminantes.
*   **Auditoría de Mocks (TDD de Calidad):** Integrado a nuestra orquestación local, invocamos `scripts/audit_local_test.py` para disparar aserciones lógicas estrictas sobre la persistencia en `tests/mock_silver`. Esta lógica evalúa de manera defensiva cómo nuestras validaciones centrales suprimen los "Drop Rates" y comprueba exhaustivamente que no haya fugas relacionales en los valores fronterizos (como `energy_consumed_kwh <= 0` en instancias de CPU, que detendrían la red).

---

## 4. Conectividad Cloud: Tracción Transaccional con AWS 🌩️

Superada exitosamente la etapa de testing aislada, el motor de Spark fue enrutado orgánicamente hacia los Buckets productivos.

*   **Protocolo Unificado `s3a://`:** Modificamos el enrutamiento lógico y sustituimos las rutas locales/`s3://` al protocolo puro `s3a://` implementado sobre el sistema de archivos subyacente de Hadoop compatible nativamente.
*   **Hadoop y AWS SDK:** Realizamos la inyección directa de credenciales vía archivo `.env`, sobrescribiendo los clusters en local para forzar a Spark a utilizar explícitamente el validador estricto de accesos `EnvironmentVariableCredentialsProvider` en AWS SDK (v1/v2), puenteando el impreciso Default Provider de Java.

---

## 4.1 Cambio funcional: `zones/catalog` removido

Para estabilizar el flujo, se eliminó por completo la dependencia de
`electricity_maps/zones/catalog` debido a que llegaba vacío desde API.

En la implementación vigente:

*   **No se ingesta `zones/catalog` en Bronze.**
*   **No se procesa `zones/catalog` en Silver.**
*   La dimensión `reference/zones_dimension` se deriva desde endpoints activos
    (`carbon_intensity/latest`, `carbon_intensity/past`,
    `carbon_intensity/history`, `electricity_mix/latest`) y se enriquece con
    `reference/geo_cloud_to_country_and_zones.csv`.
*   Se agregaron controles explícitos de calidad y auditoría para esa nueva
    dimensión (`zone_key` no nulo/único y conteo Bronze observado).

Este cambio evita dependencias frágiles y mantiene trazabilidad completa del
origen de zonas usadas por la capa Gold.

---

## 5. Evolución a Lakehouse: Integrando Delta Lake 🧊

Los archivos Parquet estándar convencionales suponían un riesgo latente persistente para la arquitectura si el proceso resultaba volátil.

*   **Implementación ACID Completa:** Acoplamos y refactorizamos el almacenamiento general en crudo a ecosistema `delta`, garantizando operaciones Aisladas y Atómicas: cualquier persistencia asíncrona truncada se convertirá transparentemente en una transacción nula sin volcar suciedad ni "Data Swamps" corruptos.
*   **Idempotencia con ReplaceWhere:** La capa analítica (`audit.py`), concebida para ser orquestable cíclicamente, presentaba problemas de duplicidad en filas al repetirse. Acabamos con las inserciones colisionadas acoplando Delta con un sobreescrito dinámico basado en particiones (`replaceWhere="execution_date = '{metadata}'"`), alcanzando en toda su gloria el Criterio de Aceptación transaccional asignado en Jira sobre la idempotencia productiva.

---

## 6. Troubleshooting Avanzado: Resolvimiento Senior Level 🧩

Empujando hacia la alta frecuencia el motor analítico, descubrimos discrepancias críticas de infraestructura, todas solucionadas íntegramente gracias al expertise de ingeniería:

### Desafío 1: El _Dependency Hell_ (PySpark 4.0 Alpha vs. Delta)
> **Problema:** Módulos que no hallaban compatibilidad por depreciación. Las compilaciones recientes Catalyst de la última release fallaban al cruzarse con el framework subyacente en la métrica validada (`Delta 3.2.0`).
> **Solución:** Elaboramos un "Downgrade y Bloqueo" drástico y limpio reenfocando nuestras interfaces de procesamiento hacia la versión más blindada de uso LTS: **Python 3.11** a través de un estricto `requirements.txt` en combinación manual bajo entorno Spark `3.5.1`.

### Desafío 2: Catalyst RecursionError (Stack Overflow)
> **Problema:** En un subset de datos de mapeos geográficos nos vimos contra un muro en memoria dinámica del clúster derivado de tener 400 iteraciones lógicas en bucles clásicos (`.union()`). A pesar de abstraerlo vectorial y compactadamente en iteradores paralelos de Comprensión (via `F.array`), el parser CloudPickle y Py4j se corrompían internamente logrando reventar la barrera con un `Stack Overflow Error (usado: 2912 kB)`.
> **Solución:** Reasumimos la táctica. Cambiamos un acople estático por serialización Spark Dataframe Nativa estructurando todo velozmente por medio puramente transaccional: una lógica escalar vectorial que eliminó completamente la sobrecarga sobre el AST al serializar el JVM driver.

### Desafío 3: Compatibilidad de Distutils (World Bank Dataset)
> **Problema:** En pleno volcado del batch estacional hacia el lakehouse, hallamos desactualizaciones por obsolescencia tecnológica (paquete default removido) para extraer módulos puros.
> **Solución:** Aportamos solidez y retrocompatibilidad dictaminando obligatoriamente a la persistencia del gestor explícito `setuptools` para permitir descargas estáticas nativas de Pandas.

---

## 7. Orquestación y Optimización Final 🚀

*   **Defensa Multi-DAG interactiva (Local):** Diseñamos las llamadas lógicas simuladas tolerante a cortes, evaluando cascadas vía Batch Processing de PowerShell: `bronze_to_silver → validations → audit`, garantizando que si uno erradica o interrumpe un job asumiendo `$LASTEXITCODE -ne 0`, frena totalmente evitando corrupción.
*   **Inyección de Memoria (8GB RAM):** Un cuello de botella inicial reducía veloz y agresivamente los Row Groups configurados de Parket debido a la alerta temporal `MemoryManager`. Modificamos la chispa de instanciación insertando `.config("spark.driver.memory", "8g")`, agilizando profundamente su latencia.
*   **El _Small Files Problem_:** Refinamos el diseño físico local sobre tablas estables dimensionales. Erradicamos decenas de particiones microscópicas del `world_bank` forzando aglomeración masiva en `coalesce` y obligando reacomodos temporales hacia archivos pesados optimizando el Storage Cost en AWS y el Fetch Load del Delta Reader.

---

*Proyecto diseñado bajo la robustez tecnológica validada por mejores prácticas Senior en Data Engineering e inyección distribuida Cloud de Green AI.*
