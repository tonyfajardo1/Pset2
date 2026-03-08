# GUIA DE EVIDENCIAS Y SCREENSHOTS - USFQ Data Mining PSet 02

## Resumen de Screenshots Requeridos

Esta guia detalla exactamente que capturas tomar y donde encontrarlas para documentar el trabajo.

---

## 1. MAGE SECRETS (Requerido por PDF)

### Screenshot 1.1: Mage Secrets Configuration
**URL:** http://localhost:6789/settings/secrets

**Pasos:**
1. Abrir Mage UI en http://localhost:6789
2. Click en icono de Settings (engranaje) en el menu lateral
3. Seleccionar "Secrets"
4. Tomar screenshot mostrando los secrets configurados:
   - POSTGRES_HOST
   - POSTGRES_PORT
   - POSTGRES_DB
   - POSTGRES_USER
   - POSTGRES_PASSWORD

---

## 2. TRIGGERS EN MAGE (Requerido por PDF)

### Screenshot 2.1: Schedule Trigger (ingest_monthly)
**URL:** http://localhost:6789/pipelines/ingest_bronze/triggers

**Pasos:**
1. Ir a Pipelines > ingest_bronze > Triggers
2. Tomar screenshot mostrando:
   - Nombre: ingest_monthly
   - Tipo: schedule
   - Status: active
   - Frecuencia: @weekly

### Screenshot 2.2: Event Trigger (dbt_after_ingest)
**URL:** http://localhost:6789/pipelines/dbt_after_ingest/triggers

**Pasos:**
1. Ir a Pipelines > dbt_after_ingest > Triggers
2. Tomar screenshot mostrando:
   - Nombre: dbt_after_ingest
   - Tipo: event
   - Event: pipeline_run_completed
   - Pipeline padre: ingest_bronze

---

## 3. PARTICIONAMIENTO (25 puntos - Muy importante)

### Screenshot 3.1: Estructura de Particiones en PostgreSQL
**Ejecutar en pgAdmin o psql:**

```sql
SELECT
    parent.relname AS tabla_padre,
    child.relname AS particion,
    pg_get_expr(child.relpartbound, child.oid) AS expresion_particion
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp ON parent.relnamespace = nmsp.oid
WHERE nmsp.nspname = 'gold'
ORDER BY parent.relname, child.relname;
```

**Tomar screenshot mostrando:**
- fct_trips: 48 particiones RANGE (mensuales 2022-2025)
- dim_zone: 4 particiones HASH
- dim_service_type: 2 particiones LIST (yellow, green)
- dim_payment_type: 3 particiones LIST (card, cash, other)

### Screenshot 3.2: Detalle de Particionamiento por Tabla
**Ejecutar:**

```sql
-- Para fct_trips (RANGE)
\d+ gold.fct_trips

-- Para dim_zone (HASH)
\d+ gold.dim_zone

-- Para dim_service_type (LIST)
\d+ gold.dim_service_type

-- Para dim_payment_type (LIST)
\d+ gold.dim_payment_type
```

### Screenshot 3.3: Partition Pruning (MUY IMPORTANTE)
**Ejecutar:**

```sql
-- Demostrar que solo accede a 1 particion
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM gold.fct_trips
WHERE pickup_date_key BETWEEN '2024-03-01' AND '2024-03-31';
```

**Debe mostrar:**
- "Partitions selected: 1" o similar
- Solo accede a `fct_trips_2024_03`
- NO escanea todas las 48 particiones

### Screenshot 3.4: Conteo de registros por particion
**Ejecutar:**

```sql
SELECT
    tableoid::regclass AS partition_name,
    COUNT(*) AS rows
FROM gold.fct_trips
GROUP BY tableoid
ORDER BY partition_name
LIMIT 20;
```

---

## 4. DBT LOGS Y TESTS (Requerido por PDF)

### Screenshot 4.1: dbt test exitoso
**Ejecutar en contenedor Mage:**

```bash
docker exec nyc_mage bash -c "cd /home/src/nyc_trips_pipeline/dbt_project && dbt test"
```

**Tomar screenshot mostrando:**
- 42 tests passed (o similar)
- 0 failures

### Screenshot 4.2: dbt run gold models
**Ejecutar:**

```bash
docker exec nyc_mage bash -c "cd /home/src/nyc_trips_pipeline/dbt_project && dbt run --select gold"
```

### Screenshot 4.3: Lineage en Mage
**URL:** http://localhost:6789/pipelines/dbt_after_ingest/edit

**Tomar screenshot del DAG mostrando:**
- dbt_build_silver -> dbt_build_gold -> quality_checks

---

## 5. MODELO ESTRELLA (20 puntos)

### Screenshot 5.1: Diagrama del Modelo Estrella
Crear diagrama (puede ser en draw.io, dbdiagram.io, o a mano) mostrando:

```
                    +------------------+
                    |   dim_date       |
                    +------------------+
                           |
+---------------+    +-------------+    +------------------+
| dim_zone      |----| fct_trips   |----|  dim_payment_type|
+---------------+    +-------------+    +------------------+
                           |
                    +------------------+
                    | dim_service_type |
                    +------------------+
```

### Screenshot 5.2: Estructura de fct_trips
**Ejecutar:**

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'gold' AND table_name = 'fct_trips'
ORDER BY ordinal_position;
```

### Screenshot 5.3: Verificar Foreign Keys (joins)
**Ejecutar:**

```sql
-- Verificar que los joins funcionan
SELECT
    f.trip_key,
    f.pickup_date_key,
    dz.zone_name AS pickup_zone,
    dst.service_name,
    dpt.payment_type_name
FROM gold.fct_trips f
JOIN gold.dim_zone dz ON f.pu_zone_key = dz.zone_key
JOIN gold.dim_service_type dst ON f.service_type = dst.service_type
JOIN gold.dim_payment_type dpt ON f.payment_type_id = dpt.payment_type_id
LIMIT 5;
```

---

## 6. PIPELINE RUNS EN MAGE

### Screenshot 6.1: Pipeline Runs History
**URL:** http://localhost:6789/pipelines

**Tomar screenshot mostrando ejecuciones exitosas de:**
- ingest_bronze
- dbt_after_ingest

---

## 7. NOTEBOOK 20 PREGUNTAS

### Screenshot 7.1: Notebook en Jupyter/Mage
**URL:** http://localhost:6789/files/data_analysis_20_questions.ipynb

**Tomar screenshots de:**
- Al menos 5 preguntas ejecutadas con resultados
- La pregunta de Partition Pruning con EXPLAIN

---

## 8. DOCKER COMPOSE Y ARQUITECTURA

### Screenshot 8.1: docker-compose.yml
**Mostrar el archivo docker-compose.yml con:**
- postgres (puerto 5432)
- pgadmin (puerto 5050)
- mage (puerto 6789)

### Screenshot 8.2: Contenedores corriendo
**Ejecutar:**

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

## COMANDOS UTILES PARA VERIFICACION

```bash
# Ver particiones
docker exec nyc_postgres psql -U postgres -d nyc_trips -c "
SELECT relname, relkind
FROM pg_class
WHERE relnamespace = 'gold'::regnamespace
ORDER BY relname;"

# Contar datos en gold
docker exec nyc_postgres psql -U postgres -d nyc_trips -c "
SELECT
    'fct_trips' as table_name, COUNT(*) as rows FROM gold.fct_trips
UNION ALL
SELECT 'dim_zone', COUNT(*) FROM gold.dim_zone
UNION ALL
SELECT 'dim_service_type', COUNT(*) FROM gold.dim_service_type
UNION ALL
SELECT 'dim_payment_type', COUNT(*) FROM gold.dim_payment_type;"

# Verificar triggers en Mage (via API)
curl -s http://localhost:6789/api/pipelines/ingest_bronze/triggers | python -m json.tool

# Ver dbt models
docker exec nyc_mage ls -la /home/src/nyc_trips_pipeline/dbt_project/models/gold/
```

---

## CHECKLIST FINAL

- [ ] Screenshot Mage Secrets
- [ ] Screenshot Schedule Trigger
- [ ] Screenshot Event Trigger
- [ ] Screenshot Lista de Particiones (query pg_inherits)
- [ ] Screenshot Partition Pruning (EXPLAIN ANALYZE)
- [ ] Screenshot dbt test passed
- [ ] Screenshot dbt run gold
- [ ] Screenshot Modelo Estrella (diagrama)
- [ ] Screenshot Estructura fct_trips
- [ ] Screenshots Notebook (5+ preguntas)
- [ ] Screenshot docker ps
- [ ] Screenshot Pipeline DAG en Mage

---

## NOTAS IMPORTANTES

1. **Partition Pruning**: Es CRITICO mostrar el EXPLAIN con "Partitions selected: 1" o similar
2. **Triggers**: Deben estar ACTIVE (no solo configurados)
3. **Modelo Estrella**: Mostrar que fct_trips tiene FKs a las dimensiones
4. **dbt tests**: Todos deben pasar (0 failures)
