# USFQ Data Mining - PSet 02

## 1. Arquitectura (bronze/silver/gold) + diagrama textual

Pipeline medallion sobre PostgreSQL orquestado con Mage y transformaciones en dbt.

```text
raw_data (parquet/csv)
        |
        v
Mage ingest_bronze
  - load_bronze_parquet
  - transform_bronze
  - export_bronze_postgres
        |
        v
PostgreSQL bronze schema
  - bronze.yellow_trips
  - bronze.green_trips
  - bronze.taxi_zone_lookup
  - bronze.coverage
        |
        v
Mage dbt_build_silver -> dbt run --select silver
        |
        v
PostgreSQL silver schema (views)
  - silver_yellow_trips
  - silver_green_trips
  - silver_trips_unified
        |
        v
Mage dbt_build_gold
  1) create_partitions
  2) dbt run --select gold
        |
        v
PostgreSQL gold schema (tables, star schema)
  - fct_trips (RANGE)
  - dim_zone (HASH)
  - dim_service_type (LIST)
  - dim_payment_type (LIST)
  - dim_date
  - dim_vendor
        |
        v
Mage quality_checks -> dbt test
```

## 2. Tabla de cobertura por mes y servicio

La cobertura oficial se mantiene en `bronze.coverage` con las columnas requeridas:

- `year_month`
- `service_type`
- `status` (`loaded` | `missing` | `failed`)
- `row_count` (conteo real)

Idempotencia implementada en ingesta: antes de insertar cada mes/servicio se ejecuta
`DELETE FROM bronze.<service>_trips WHERE source_month = '<YYYY-MM>'`.

Consulta para exportar la tabla completa:

```sql
SELECT year_month, service_type, status, row_count
FROM bronze.coverage
ORDER BY year_month, service_type;
```

## 3. Como levantar el stack (docker compose up)

```bash
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Servicios esperados:

- `nyc_postgres` (5432)
- `nyc_pgadmin` (5050)
- `nyc_mage` (6789)

## 4. Pipelines de Mage y que hace cada uno

- `ingest_bronze`: ingesta mensual de parquet, idempotencia por `source_month` y `service_type`, y actualizacion de cobertura.
- `dbt_build_silver`: ejecuta modelos silver (materialized view).
- `dbt_build_gold`: crea particiones y ejecuta modelos gold (materialized table).
- `quality_checks`: ejecuta `dbt test` y falla si algun test falla.
- `dbt_after_ingest`: encadena `dbt_build_silver -> dbt_build_gold -> quality_checks`.

## 5. Triggers y que disparan

- `ingest_monthly` (schedule trigger, `@weekly`): ejecuta `ingest_bronze`.
- `dbt_after_ingest` (schedule/API trigger): ejecuta la cadena `dbt_build_silver -> dbt_build_gold -> quality_checks`.

Evidencias:

- `Evidencias/Ingest Bronze Pipelines.png`
- `Evidencias/DBT After Ingest.png`

## 6. Gestion de secretos

Secretos esperados en Mage Secrets (sin valores):

- `POSTGRES_HOST`: host de PostgreSQL
- `POSTGRES_PORT`: puerto de PostgreSQL
- `POSTGRES_DB`: base de datos
- `POSTGRES_USER`: usuario
- `POSTGRES_PASSWORD`: password

Notas:

- `.env` no se versiona.
- `.env.example` si se versiona.
- No se guardan valores de secretos en el repo.

## 7. Particionamiento

### 7.1 Evidencias con `\d+`

Ejecutar en `psql`:

```sql
\d+ gold.fct_trips
\d+ gold.dim_zone
\d+ gold.dim_service_type
\d+ gold.dim_payment_type
```

Debe observarse:

- `gold.fct_trips` -> `Partition key: RANGE (pickup_date_key)`
- `gold.dim_zone` -> `Partition key: HASH (zone_key)`
- `gold.dim_service_type` -> `Partition key: LIST (service_type)`
- `gold.dim_payment_type` -> `Partition key: LIST (payment_type)`

### 7.2 Evidencias con EXPLAIN (ANALYZE, BUFFERS)

Consulta 1 (RANGE pruning en hechos):

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM gold.fct_trips
WHERE pickup_date_key BETWEEN '2024-03-01' AND '2024-03-31';
```

Consulta 2 (HASH pruning en dimension):

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM gold.dim_zone
WHERE zone_key = 132;
```

Interpretacion esperada (2-4 lineas):

- En la consulta de hechos, el planner restringe el escaneo a la particion del mes filtrado (partition pruning por rango).
- En la consulta de zona, el planner dirige la busqueda a la particion hash correspondiente al `zone_key`.
- Esto reduce bloques leidos y evita escaneo completo de particiones no relevantes.

## 8. dbt: materializations y logs

Materializaciones:

- Silver: `view`
- Gold: `table`

Comandos de referencia (ejecutados desde Mage):

```bash
dbt run --select silver --profiles-dir .
dbt run --select gold --profiles-dir .
dbt test --profiles-dir .
```

Resultado final de calidad (evidencia real):

- `Finished running 44 data tests ...`
- `Completed successfully`
- `Done. PASS=44 WARN=0 ERROR=0 SKIP=0 TOTAL=44`

Captura:

- `Evidencias/DBT Test Final PASS.png`

## 9. Troubleshooting

1. `dbt test` falla por relaciones de llaves
   - Verificar que `dim_zone`, `dim_date` y `fct_trips` se construyeron en el orden correcto.
   - Reejecutar `dbt run --select gold` y luego `dbt test`.

2. Error de tabla no encontrada en primera corrida (`bronze.taxi_zone_lookup`)
   - Validar que `load_bronze_parquet` corra antes de modelos silver.
   - Confirmar que la carga de `taxi_zone_lookup` se complete en bronze.

3. Problemas de memoria en carga de parquet
   - Ejecutar ingesta por chunks (ya implementado en exporter).
   - Evitar correr ingesta y dbt al mismo tiempo en hardware limitado.

## 10. Checklist de aceptacion (Seccion 16)

- [x] Docker Compose levanta Postgres + Mage
- [x] Credenciales en Mage Secrets y `.env` (solo `.env.example` en repo)
- [x] Pipeline `ingest_bronze` mensual e idempotente + tabla de cobertura
- [x] dbt corre dentro de Mage: `dbt_build_silver`, `dbt_build_gold`, `quality_checks`
- [x] Silver materialized = views; Gold materialized = tables
- [x] Gold tiene esquema estrella completo
- [x] Particionamiento: RANGE en `fct_trips`, HASH en `dim_zone`, LIST en `dim_service_type` y `dim_payment_type`
- [x] README incluye `\d+` y `EXPLAIN (ANALYZE, BUFFERS)` con pruning
- [x] dbt test pasa desde Mage
- [x] Notebook responde 20 preguntas usando solo `gold.*`
- [x] Triggers configurados y evidenciados

## 11. Documentos de apoyo

- Plan previo a validacion (pasos 1-3): `PLAN_PRE_VALIDACION_PASOS_1_2_3.md`
- Script de consultas para evidencias: `SQL_EVIDENCIAS_PSET2.sql`

## 12. Inventario de evidencias (capturas reales)

Evidencias disponibles en `Evidencias/`:

- `Evidencias/Mage Secrets.png` (secrets configurados)
- `Evidencias/Ingest Bronze Pipelines.png` (pipeline y/o corrida de ingest)
- `Evidencias/Ingest Bronze.png`
- `Evidencias/Ingest Bronze 2.png`
- `Evidencias/Ingest Bronze 3.png`
- `Evidencias/DBT After Ingest.png` (cadena dbt ejecutada)
- `Evidencias/DBT Test Final PASS.png` (PASS=44, ERROR=0)
- `Evidencias/Partition Strategy Gold.png` (RANGE/HASH/LIST)
- `Evidencias/Partition Keys Gold.png` (keys de particion)
- `Evidencias/Partition Children Gold.png` (particiones hijas)
- `Evidencias/Explain Pruning FCT Trips.png` (pruning por RANGE)
- `Evidencias/Explain Pruning Dim Zone.png` (pruning por HASH)
- `Evidencias/Coverage Status Summary.png` (loaded/missing)
- `Evidencias/Coverage Detail.png` (detalle por mes/servicio)

Resumen validado con evidencia:

- Calidad: `dbt test` completo en PASS (`44/44`).
- Particionamiento: estrategias y claves correctas en tablas gold.
- Pruning: evidenciado en hechos (`fct_trips_2024_03`) y dimension hash (`dim_zone_p3`).
- Cobertura: `loaded=94`, `missing=2`; `row_count` real por mes/servicio.
