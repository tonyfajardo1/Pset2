"""
Custom Block: Encadena dbt_build_silver -> dbt_build_gold -> quality_checks
Este pipeline se ejecuta después de que ingest_bronze termina exitosamente.
"""
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.shared.secrets import get_secret_value
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def run_dbt_pipeline_chain(*args, **kwargs):
    """
    Ejecuta en orden:
    1. dbt_build_silver
    2. dbt_build_gold  
    3. quality_checks (dbt test)
    
    Este block cumple con el requisito de dbt_after_ingest (Event trigger/pipeline chaining)
    """
    base_path = '/home/src/nyc_trips_pipeline'
    
    pipelines_to_run = [
        ('dbt_build_silver', 'Ejecutando dbt seed + dbt run --select silver'),
        ('dbt_build_gold', 'Ejecutando create_partitions + dbt run --select gold'),
        ('quality_checks', 'Ejecutando dbt test'),
    ]
    
    results = []
    
    for pipeline_name, description in pipelines_to_run:
        print(f"\n{'='*60}")
        print(f"{description}")
        print(f"{'='*60}")
        
        try:
            pipeline = Pipeline.get(
                pipeline_name,
                repo_path=base_path
            )
            pipeline.execute()
            results.append({'pipeline': pipeline_name, 'status': 'success'})
            print(f"[OK] {pipeline_name} completado")
        except Exception as e:
            results.append({'pipeline': pipeline_name, 'status': 'failed', 'error': str(e)})
            print(f"[ERROR] {pipeline_name} falló: {e}")
            raise
    
    print(f"\n{'='*60}")
    print("CADENA DBT COMPLETADA")
    print(f"{'='*60}")
    
    return {
        'status': 'success',
        'results': results
    }


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success'
