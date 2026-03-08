# Plan previo a validacion ejecutable (Pasos 1, 2 y 3)

Este documento te deja todo listo antes de ejecutar la validacion final.

## Paso 1) Preparar corrida end-to-end en Mage

Objetivo: que `ingest_bronze -> dbt_after_ingest -> dbt_build_silver -> dbt_build_gold -> quality_checks` quede consistente.

### 1.1 Verificar pipelines y bloques en Mage UI

- `ingest_bronze`
  - `load_bronze_parquet`
  - `transform_bronze`
  - `export_bronze_postgres`
- `dbt_after_ingest`
  - `dbt_pipeline_chain`
- `dbt_build_silver`
  - modelos silver
- `dbt_build_gold`
  - `create_partitions`
  - modelos gold
- `quality_checks`
  - `dbt_test`

### 1.2 Verificar triggers activos

- Schedule trigger: `ingest_monthly` en `ingest_bronze`
- Event trigger: `dbt_after_ingest` disparado por `ingest_bronze` exitoso

### 1.3 Verificar secrets cargados en Mage

Nombres esperados:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

### 1.4 Verificar precondiciones de infraestructura

- Contenedores levantados: `nyc_postgres`, `nyc_pgadmin`, `nyc_mage`
- Puerto Mage: `6789`
- Puerto Postgres: `5432`

## Paso 2) Preparar calidad (`dbt test`) antes de ejecutar

Objetivo: minimizar fallos de pruebas en la corrida final.

### 2.1 Confirmar que el pipeline falle si un test falla

Ya implementado en:

- `mage_data/nyc_trips_pipeline/custom/dbt_test.py`
- `mage_data/nyc_trips_pipeline/data_loaders/run_dbt_tests.py`

### 2.2 Revisar cobertura de tests requeridos por PDF

Archivo principal:

- `mage_data/nyc_trips_pipeline/dbt_project/models/gold/schema.yml`

Debe cubrir:

- `unique` + `not_null` en `trip_key`, `zone_key`, `date_key`
- `relationships` de `fct_trips` hacia dimensiones
- `accepted_values` en `service_type` y `payment_type`

### 2.3 Revisar reglas minimas de silver

Archivo:

- `mage_data/nyc_trips_pipeline/dbt_project/tests/assert_silver_temporal_and_non_negative.sql`

Valida:

- fechas no nulas
- pickup <= dropoff
- distancia y monto no negativos

## Paso 3) Preparar evidencias para README/entrega

Objetivo: dejar lista la captura de evidencia obligatoria del PDF.

### 3.1 Evidencias de particionamiento

Guardar capturas/salidas de:

- `\d+ gold.fct_trips`
- `\d+ gold.dim_zone`
- `\d+ gold.dim_service_type`
- `\d+ gold.dim_payment_type`

### 3.2 Evidencias de pruning

Guardar salidas de:

- `EXPLAIN (ANALYZE, BUFFERS)` con filtro mensual en `gold.fct_trips`
- `EXPLAIN (ANALYZE, BUFFERS)` por `zone_key` en `gold.dim_zone`

### 3.3 Evidencias de calidad

Guardar salida/log de:

- `dbt run --select silver`
- `dbt run --select gold`
- `dbt test`

### 3.4 Evidencias de orquestacion y seguridad

Capturas:

- Secrets de Mage (valores ocultos)
- Triggers activos (`ingest_monthly` y `dbt_after_ingest`)
- Historial de corrida end-to-end en Mage

## Formato sugerido de nombres de archivo para capturas

- `01_mage_secrets.png`
- `02_trigger_ingest_monthly.png`
- `03_trigger_dbt_after_ingest.png`
- `04_dplus_fct_trips.png`
- `05_dplus_dim_zone.png`
- `06_dplus_dim_service_type.png`
- `07_dplus_dim_payment_type.png`
- `08_explain_fct_trips_pruning.png`
- `09_explain_dim_zone_pruning.png`
- `10_dbt_test_pass.png`
- `11_mage_end_to_end_logs.png`
