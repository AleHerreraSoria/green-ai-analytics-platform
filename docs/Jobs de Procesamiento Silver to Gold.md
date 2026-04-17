## Jobs de Procesamiento Silver -> Gold

En esta etapa desarrollé un script en Spark que transforma los datos de la capa Silver en un modelo final en Gold, orientado a análisis de negocio.

Primero cargué los datasets de uso (usage_logs), geolocalización, precios de energía, emisiones (MLCO2) y costos de instancias EC2 desde S3. Luego normalicé las claves (como region, country e instance_type) para asegurar consistencia en los joins.

Apliqué deduplicación en las tablas de referencia para evitar relaciones 1:N que generaban duplicados en el dataset final. Después realicé los joins entre las distintas fuentes, cuidando que no se generen columnas duplicadas ni ambigüedades (problema que inicialmente rompía el pipeline).

Una vez integrados los datos, calculé las métricas de negocio:

costo energético (cost_energy)
costo de cómputo (cost_compute)
emisiones de CO2 (emissions_co2)

Finalmente, escribí la tabla fact_ai_jobs en la capa Gold (S3 en formato parquet) y validé los resultados en Athena, verificando:

cantidad total de registros
ausencia de valores nulos en métricas clave
ausencia de duplicados (unicidad por session_id)
✅ Resultado

Se obtuvo una tabla Gold consistente, sin duplicados y lista para responder preguntas de negocio sobre costos y emisiones en workloads de IA.