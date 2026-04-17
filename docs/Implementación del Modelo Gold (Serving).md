## Implementación del Modelo Gold (Serving)

En esta tarea se diseñó e implementó la capa Gold del pipeline de datos, orientada al análisis de negocio.

Se creó una base de datos gold en Athena y se definieron tablas externas almacenadas en Amazon S3 (bucket Gold), siguiendo un enfoque de modelado dimensional tipo Kimball.

Se implementaron las siguientes tablas:

fact_ai_jobs: tabla de hechos que contiene métricas clave de las ejecuciones de IA (consumo energético, duración, utilización de GPU, región, tipo de trabajo, estado, timestamp).
dim_region: dimensión geográfica con información de país, precio de electricidad e intensidad de carbono.
dim_gpu: dimensión técnica con características de hardware (modelo, TDP, capacidad de cómputo).

Las tablas fueron creadas como external tables en Athena, con almacenamiento en formato Parquet en S3, quedando listas para ser pobladas en etapas posteriores del pipeline (Silver → Gold).

Este diseño permite soportar consultas analíticas orientadas a responder preguntas de negocio relacionadas con eficiencia energética, costos y emisiones de carbono en workloads de IA.