# Bitácora de Pruebas de Resiliencia e Idempotencia

## Contexto de Testing

> Este documento detalla la metodología y los resultados de las pruebas end-to-end de idempotencia y resiliencia implementadas sobre la arquitectura Data Lakehouse (Bronze → Silver → Gold) utilizando PySpark y Delta Lake.

Los scripts de prueba se encuentran en `ci/testing/`:
- `idempotency_test.py`
- `resilience_test.py`

---

## 1. Pruebas de Idempotencia (`idempotency_test.py`)

### Objetivo
Validar empíricamente que múltiples ejecuciones de los pipelines ETL (`bronze_to_silver` y `silver_to_gold`) sobre los mismos datos en S3 no producen incremento en el conteo de filas ni introducen registros duplicados, asegurando que las operaciones `MERGE` funcionan correctamente.

### Metodología Implementada
1. Se inicializa un entorno Spark local conectado a AWS S3.
2. Se toma un snapshot inicial (Run 0) del estado de todas las tablas Silver (13) y Gold (7) leyendo directamente de Delta Lake.
3. Se ejecuta el pipeline completo (Bronze → Silver → Gold) invocando dinámicamente los scripts ETL reales.
4. Se vuelve a tomar un snapshot del estado (Run 1) calculando `row_count` y evaluando `dup_count` mediante agrupaciones por clave primaria (`groupBy(pk).count() > 1`).
5. Se repite el proceso para una ejecución adicional (Run 2).
6. Se comparan todos los snapshots entre sí.

### Criterios de Éxito
- **Regresiones = 0**: El `row_count` debe ser idéntico en cada ejecución (Run N vs Run N-1).
- **Duplicados = 0**: El `dup_count` de cada tabla debe ser estrictamente 0 en cada lote.

### Historial de Resultados

| Fecha | Entorno | Ejecuciones | Estado | Notas y Correcciones |
|-------|---------|-------------|--------|----------------------|
| 28/04/2026 | Local/S3 | 2 Runs | ✅ PASS | El test descubrió el error `multipleSourceRowMatchingTargetRowInMergeException`. Se corrigió inyectando `df.dropDuplicates(merge_keys)` antes del `MERGE` en `writer.py` y `silver_to_gold.py`. Post-corrección, `regressions=0` y `dup_violations=0`. |

---

## 2. Pruebas de Resiliencia (`resilience_test.py`)

### Objetivo
Garantizar que el ecosistema Delta Lake se recupera de escenarios de fallo catastróficos, corrupción de esquema y escrituras interrumpidas, sin dejar datos huérfanos ni corromper las tablas de destino.

### Escenarios Implementados
Las pruebas se ejecutan focalizándose en una tabla de control (`mlco2/gpus` en Silver) para simular inyecciones anómalas:

1. **Schema Poisoning (`schema_poison`)**: 
   - *Acción*: Se intenta sobrescribir la tabla inyectando un DataFrame con una columna extra e incompatible (tipo `ARRAY<string>`).
   - *Validación*: Delta Lake debe rechazar el esquema. El retry del ETL debe ser exitoso y la tabla debe permanecer intacta.
   
2. **Inyección de PK Nula (`null_pk_injection`)**:
   - *Acción*: Se intenta forzar un `MERGE` directo con registros cuya clave primaria es `NULL`.
   - *Validación*: Delta descarta los nulos silenciosamente en la condición `ON`. Tras el retry, la tabla debe tener 0 registros con PK nula.
   
3. **Escritura Parcial Interrumpida (`partial_write_orphan`)**:
   - *Acción*: Se inyecta un archivo Parquet corrupto (0-bytes) directamente en la ruta de S3, simulando la muerte de un nodo durante la escritura.
   - *Validación*: Delta Lake debe ignorar el archivo corrupto porque no existe un commit en el `_delta_log`. El retry del ETL debe funcionar sin problemas.

### Historial de Resultados

| Fecha | Escenario | Estado pre-fault | Estado post-retry | Archivos Temp | Legible | Status |
|-------|-----------|------------------|-------------------|---------------|---------|--------|
| 28/04/2026 | `schema_poison` | 48 filas | 48 filas | 0 | Sí | ✅ PASS |
| 28/04/2026 | `null_pk_injection` | 48 filas | 48 filas | 0 | Sí | ✅ PASS |
| 28/04/2026 | `partial_write_orphan` | 48 filas | 48 filas | 0 | Sí | ✅ PASS |

---

## 3. Conclusiones y Próximos Pasos

La transición de escrituras basadas en `overwrite` a una arquitectura de transacciones **Delta Lake MERGE** ha demostrado ser robusta frente a re-ejecuciones (idempotencia) y resistente a corrupciones a nivel de archivo (resiliencia).

**Mejoras recomendadas a futuro:**
- Ejecutar estos tests de forma periódica dentro del CI/CD para evitar regresiones de código.
- Escalar los tests de resiliencia para incluir la interrupción del `_delta_log` mismo y probar mecanismos de recuperación como `FSCK REPAIR TABLE`.
