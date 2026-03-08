# Checklist de Cumplimiento PSet 02 (Pendientes)

Base de revision: `DM-PSet-2.pdf` (contenido extraido en `pdf_output.txt`).

## 1) Bloqueantes de entrega (hacer primero)

- [x] Crear `README.md` completo con el orden exacto pedido en la Seccion 12 del PDF.
- [x] Incluir en `README.md` la tabla de cobertura por mes/servicio con columnas: `year_month`, `service_type`, `status`, `row_count`.
- [ ] Incluir en `README.md` evidencias de particionamiento con `\d+` para:
  - `gold.fct_trips` (RANGE)
  - `gold.dim_zone` (HASH)
  - `gold.dim_service_type` (LIST)
  - `gold.dim_payment_type` (LIST)
- [x] Incluir en `README.md` 2 consultas con `EXPLAIN (ANALYZE, BUFFERS)` + explicacion breve de pruning (2-4 lineas).
- [x] Agregar en `README.md` logs/snippets de `dbt run` y `dbt test` ejecutados desde Mage.
- [x] Agregar en `README.md` seccion de troubleshooting con minimo 3 problemas comunes y solucion.

## 2) Calidad obligatoria (dbt + Mage)

- [x] Hacer que `quality_checks` falle realmente si `dbt test` falla (actualmente no bloquea la corrida).
- [ ] Agregar tests dbt obligatorios en Gold:
  - [x] `unique` + `not_null` en `gold.fct_trips.trip_key`
  - [x] `unique` + `not_null` en `gold.dim_zone.zone_key`
  - [x] `unique` + `not_null` en `gold.dim_date.date_key`
  - [ ] `relationships`:
    - [x] `fct_trips.pu_zone_key -> dim_zone.zone_key`
    - [x] `fct_trips.do_zone_key -> dim_zone.zone_key`
    - [x] `fct_trips.pickup_date_key -> dim_date.date_key`
  - [x] `accepted_values` en `dim_service_type.service_type` = `('yellow','green')`
  - [x] `accepted_values` en `dim_payment_type.payment_type` (dominio documentado)

## 3) Orquestacion obligatoria

- [x] Verificar/ajustar `dbt_build_gold` para que ejecute en orden obligatorio:
  1. SQL de particionamiento
  2. `dbt run --select gold`
  3. `quality_checks` (`dbt test`)
- [ ] Confirmar que la ejecucion end-to-end depende de Mage (sin pasos manuales fuera de Mage).

## 4) Particionamiento y modelo Gold

- [ ] Confirmar que las tablas particionadas se crean y se pueblan por el flujo oficial (no solo por scripts sueltos).
- [ ] Validar que `fct_trips` mantiene granularidad `1 fila = 1 viaje`.
- [ ] Verificar que las dimensiones obligatorias del esquema estrella esten completas y consistentes:
  - [ ] `dim_date`
  - [ ] `dim_zone`
  - [ ] `dim_service_type`
  - [ ] `dim_payment_type`
  - [ ] `dim_vendor` (si aplica)

## 5) Bronze / cobertura / idempotencia

- [x] Corregir tabla de cobertura para que `row_count` sea conteo real (no bandera 1/0).
- [x] Alinear `status` de cobertura a lo requerido por PDF: `loaded`, `missing`, `failed` (sin `pending`).
- [x] Corregir validacion inicial de `taxi_zone_lookup` para evitar fallo en primera corrida si la tabla no existe.
- [x] Documentar claramente en README la estrategia de idempotencia (DELETE por `source_month` + `service_type`).

## 6) Seguridad y secretos

- [x] Quitar secretos/versionado sensible del repo (`mage_data/secrets/postgres.yaml` no debe versionarse con password real).
- [x] Evitar credenciales hardcodeadas en notebook/codigo (usar variables/secrets).
- [ ] Mantener `.env` fuera del repo y conservar solo `.env.example`.
- [x] Documentar en README la lista de secretos (solo nombres y proposito, sin valores).

## 7) Notebook de 20 preguntas (Seccion 14)

- [x] Ajustar el notebook para responder exactamente las 20 preguntas del PDF (mismo enfoque tematico y metrica).
- [ ] En cada pregunta incluir:
  - [x] Query SQL
  - [x] 1-3 lineas de interpretacion
  - [x] Tablas usadas (comentario o markdown)
- [x] Mantener analisis usando exclusivamente `gold.*`.

## 8) Evidencias y capturas (entrega)

- [ ] Capturas de Mage Secrets (valores ocultos).
- [ ] Capturas de triggers configurados y activos.
- [ ] Capturas/logs de corrida end-to-end.
- [ ] Capturas de particiones y pruning.
- [ ] Evidencia de `dbt test` passing desde Mage.

## 9) Checklist final de aceptacion (para copiar al README)

- [ ] Docker Compose levanta Postgres + Mage.
- [ ] Credenciales en Mage Secrets y `.env` (solo `.env.example` en repo).
- [ ] `ingest_bronze` mensual e idempotente + tabla cobertura.
- [ ] `dbt_build_silver`, `dbt_build_gold`, `quality_checks` corren en Mage.
- [ ] Silver = views; Gold = tables.
- [ ] Esquema estrella Gold completo.
- [ ] Particionamiento RANGE/HASH/LIST implementado y evidenciado.
- [ ] `README` con `\d+` y `EXPLAIN (ANALYZE, BUFFERS)` + pruning.
- [ ] `dbt test` pasa desde Mage.
- [ ] Notebook responde 20 preguntas usando solo Gold.
- [ ] Triggers configurados y evidenciados.
