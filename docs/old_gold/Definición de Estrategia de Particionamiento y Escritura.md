## Definición de Estrategia de Particionamiento y Escritura

En esta etapa se definió la estrategia de almacenamiento físico de la capa Gold, con el objetivo de optimizar el rendimiento de consultas y garantizar consistencia en los datos.

### 🔹 Estrategia de guardado

Se utilizó el modo **overwrite** para la escritura de la tabla `fact_ai_jobs`.

**Justificación:**
- El pipeline es batch completo (no incremental)
- Garantiza consistencia total en cada ejecución
- Evita duplicados o inconsistencias acumuladas
- Simplifica la lógica de procesamiento

Se descartó:
- `append`: por riesgo de duplicación de datos  
- `overwrite partition`: innecesario para el alcance actual  

---

### 🔹 Estrategia de particionamiento

La tabla de hechos `fact_ai_jobs` se particiona por:

- `year`
- `month`

**Justificación:**
- Permite filtrar consultas por período (principal caso de uso)
- Reduce el volumen de datos escaneados en Athena
- Mejora el rendimiento y reduce costos
- Es una práctica estándar en modelos analíticos

---

### 🔹 Tablas de dimensiones

Las tablas de dimensiones no se particionan.

**Justificación:**
- Bajo volumen de datos
- Baja frecuencia de actualización
- El particionamiento no aporta mejoras relevantes

---

### 🔹 Estrategia de actualización (SCD)

No se implementa Slowly Changing Dimensions (SCD) en este proyecto.

**Justificación:**
- Se trabaja con snapshots actuales
- No se requiere historial de cambios
- Se prioriza simplicidad del modelo

**Posible mejora futura:**
- Implementar SCD Tipo 2 para precios de energía o emisiones

---

### ✅ Conclusión

Se definió una estrategia simple y eficiente basada en:
- Escritura completa (`overwrite`)
- Particionamiento temporal (`year`, `month`)
- Modelo sin SCD

Esto permite:
- Consultas más rápidas y económicas en Athena
- Datos consistentes en cada ejecución
- Un pipeline fácil de mantener y escalar