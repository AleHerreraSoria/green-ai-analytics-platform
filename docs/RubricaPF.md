## CHECKLIST – NIVEL SOBRESALIENTE

# Diseño técnico y arquitectura
x Arquitectura modular y escalable claramente definida
x Separación clara en capas: raw / processed (silver) / gold
x Diagrama de arquitectura claro, completo y entendible
x Decisiones técnicas justificadas (costos, rendimiento, mantenibilidad)
x Estructura del repo ordenada y consistente
x Nombres de carpetas/archivos claros y coherentes
x Documentación completa del diseño


# Infraestructura como código (IaC)
x Toda la infraestructura está definida con Terraform (o similar)
x Uso de versionado (Git) + PRs
x Recursos configurados con roles/permisos correctos
x Evidencia de control de costos (presupuesto en 0 o controlado)
x Aplicación de buenas prácticas básicas de seguridad
x Infraestructura reproducible (no manual)


# Ingesta de datos (RAW / S3)
x Datos organizados en S3 por capas y estructura clara
x Uso de formatos optimizados (Parquet / Open Table Format)
x Particionamiento correcto (por fecha u otro criterio útil)
 Ingesta con:
x nombre de archivo claro
x tamaño
x fecha
x Trazabilidad completa de los datos
x Manejo de chunks (si aplica)


# Procesamiento (Silver)
x Limpieza de datos completa
x Tipos de datos correctos y consistentes
x Columnas renombradas correctamente
x Eliminación de duplicados
x Filtros alineados al negocio
x Validaciones mínimas implementadas
x Datos guardados en Parquet comprimido


# Calidad de datos y gobernanza
  Controles formales implementados:
x conteos
x nulos
x duplicados
x Tipos de datos documentados
x Criterios de aceptación/rechazo definidos
x Evidencia de correcciones o resultados
x Proceso de calidad documentado


# Orquestación del pipeline
  DAGs:
x deterministas
x idempotentes
x Manejo de errores y reintentos
x Logging centralizado
x Pipeline reproducible end-to-end
x Tolerancia a fallos


# CI/CD
  Pipeline ejecuta:
☐ tests
☐ build
☐ deploy automático
☐ Infraestructura o jobs se despliegan automáticamente
☐ Uso de PR + revisión de código
☐ Código validado antes de merge


# Monitoreo y logging
x Métricas y logs consolidados (ej: CloudWatch)
x Evidencia de debugging
x Auditoría de ejecuciones
x Seguimiento del pipeline


# Capa Gold (analítica)
  Tablas Gold:
x limpias
x documentadas
x alineadas al negocio
  Responden preguntas reales del caso
  Incluye:
☐ consultas SQL
☐ notebooks de verificación
x Coherencia total con el caso (ONG / negocio)


# Performance y costos
x Uso de formatos optimizados
x Buen particionamiento
x Reducción de tiempos de ejecución
x Optimización de costos justificada
x Trade-offs explicados (ej: costo vs performance)


# Documentación y reproducibilidad
  README completo con:
☐ arquitectura
☐ decisiones técnicas
☐ Diagramas incluidos
☐ Pasos claros para correr el proyecto



☐ Proyecto reproducible de punta a punta


# Presentación
☐ Estructura clara y lógica
☐ Hay un hilo conductor
☐ Uso correcto de recursos visuales
☐ Diapositivas apoyan el contenido (no lo repiten)
☐ Discurso fluido y sin vacíos


# Trabajo en equipo
☐ Roles y responsabilidades claros
☐ Trabajo coordinado
☐ Buena comunicación
☐ Capacidad de resolver conflictos
☐ Evidencia de colaboración real


# Metodología ágil
☐ Planning realizados
☐ Retros ejecutadas
☐ Evidencia de mejoras aplicadas
☐ Adaptación a cambios