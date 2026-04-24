# Green AI Analytics Platform - Documentación

Bienvenido al directorio de documentación del proyecto. Este repositorio contiene toda la información técnica, de arquitectura y procesos necesarios para entender y operar la plataforma.

La documentación ha sido organizada en las siguientes categorías para facilitar su acceso:

## 🏛️ [Architecture](architecture/)
Documentos de diseño de alto nivel, modelos de datos y requisitos de negocio.
- [`business_questions.md`](architecture/business_questions.md): Preguntas de negocio que guía el diseño de la plataforma.
- [`business_requirements.md`](architecture/business_requirements.md): Requerimientos y visión del proyecto (KPIs).
- [`dimensional_model.md`](architecture/dimensional_model.md): Diseño del modelo dimensional para la capa Gold.
- [`streaming_ingestion_proposal.md`](architecture/streaming_ingestion_proposal.md): Propuesta de arquitectura para streaming.
- [`technical_decisions.md`](architecture/technical_decisions.md): Decisiones técnicas de la arquitectura (ADR).

## 🚰 Pipelines
Documentación específica para cada capa de procesamiento de datos en nuestra arquitectura Medallion.
- **[Bronze](pipelines/bronze/)**:
  - [`bronze_layer.md`](pipelines/bronze/bronze_layer.md): Documentación de ingesta de datos y almacenamiento en bruto.
- **[Silver](pipelines/silver/)**:
  - [`silver_layer.md`](pipelines/silver/silver_layer.md): Documentación de validación, limpieza y normalización.
  - [`silver_readme.md`](pipelines/silver/silver_readme.md): Decisiones estructurales en la capa Silver.
- **[Gold](pipelines/gold/)**:
  - [`gold_layer_architecture.md`](pipelines/gold/gold_layer_architecture.md): Arquitectura detallada de la capa de analítica y BI.
  - **[Legacy](pipelines/gold/legacy/)**: Archivos históricos de diseño e implementación original de la capa Gold (para referencia).

## 🚀 [Setup & Deployment](setup_and_deployment/)
Guías de infraestructura y despliegue.
- [`infrastructure_requirements.md`](setup_and_deployment/infrastructure_requirements.md): Requisitos de infraestructura y despliegue en AWS/Local.

## 📚 [References](references/)
Diccionarios, glosarios y otras referencias técnicas.
- [`data_dictionary.md`](references/data_dictionary.md): Diccionario de datos para todos los datasets.
- [`glossary_metrics.md`](references/glossary_metrics.md): Glosario de términos y definición de métricas.
