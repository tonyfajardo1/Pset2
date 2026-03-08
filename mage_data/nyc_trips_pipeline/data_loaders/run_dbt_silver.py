import subprocess
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def run_dbt_silver(*args, **kwargs):
    """
    Ejecuta dbt para construir la capa Silver.

    Pasos:
    1. dbt seed (carga taxi_zones)
    2. dbt run --select silver (crea vistas Silver)
    """
    dbt_project_path = "/home/src/dbt_project"

    results = {}

    # Paso 1: Ejecutar dbt seed
    print("=" * 50)
    print("PASO 1: Ejecutando dbt seed...")
    print("=" * 50)

    seed_result = subprocess.run(
        ["dbt", "seed", "--profiles-dir", "."],
        cwd=dbt_project_path,
        capture_output=True,
        text=True
    )

    print(seed_result.stdout)
    if seed_result.stderr:
        print("STDERR:", seed_result.stderr)

    results['seed'] = {
        'returncode': seed_result.returncode,
        'success': seed_result.returncode == 0
    }

    if seed_result.returncode != 0:
        raise Exception(f"dbt seed failed: {seed_result.stderr}")

    # Paso 2: Ejecutar dbt run para Silver
    print("\n" + "=" * 50)
    print("PASO 2: Ejecutando dbt run --select silver...")
    print("=" * 50)

    run_result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--select", "silver"],
        cwd=dbt_project_path,
        capture_output=True,
        text=True
    )

    print(run_result.stdout)
    if run_result.stderr:
        print("STDERR:", run_result.stderr)

    results['run_silver'] = {
        'returncode': run_result.returncode,
        'success': run_result.returncode == 0
    }

    if run_result.returncode != 0:
        raise Exception(f"dbt run silver failed: {run_result.stderr}")

    print("\n" + "=" * 50)
    print("SILVER LAYER COMPLETADA EXITOSAMENTE")
    print("=" * 50)

    return results


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is None'
    assert output.get('seed', {}).get('success'), 'dbt seed failed'
    assert output.get('run_silver', {}).get('success'), 'dbt run silver failed'
