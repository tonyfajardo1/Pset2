import subprocess
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def run_dbt_gold(*args, **kwargs):
    """
    Ejecuta dbt para construir la capa Gold.

    Pasos:
    1. Crear particiones en PostgreSQL (si no existen)
    2. dbt run --select gold (crea tablas Gold con star schema)
    """
    dbt_project_path = "/home/src/dbt_project"

    results = {}

    # Paso 1: Ejecutar dbt run para Gold
    print("=" * 50)
    print("Ejecutando dbt run --select gold...")
    print("=" * 50)

    run_result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--select", "gold"],
        cwd=dbt_project_path,
        capture_output=True,
        text=True
    )

    print(run_result.stdout)
    if run_result.stderr:
        print("STDERR:", run_result.stderr)

    results['run_gold'] = {
        'returncode': run_result.returncode,
        'success': run_result.returncode == 0
    }

    if run_result.returncode != 0:
        raise Exception(f"dbt run gold failed: {run_result.stderr}")

    print("\n" + "=" * 50)
    print("GOLD LAYER COMPLETADA EXITOSAMENTE")
    print("=" * 50)

    return results


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is None'
    assert output.get('run_gold', {}).get('success'), 'dbt run gold failed'
