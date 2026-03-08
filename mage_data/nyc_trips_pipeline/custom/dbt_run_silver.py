"""
Custom Block: Ejecuta dbt run para la capa Silver.
"""
import subprocess
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def run_dbt_silver(*args, **kwargs):
    """
    Ejecuta dbt run --select silver para crear las vistas en schema silver.
    """
    dbt_path = "/home/src/dbt_project"
    
    env = os.environ.copy()
    env['POSTGRES_HOST'] = os.getenv('POSTGRES_HOST', 'postgres')
    env['POSTGRES_PORT'] = os.getenv('POSTGRES_PORT', '5432')
    env['POSTGRES_DB'] = os.getenv('POSTGRES_DB', 'nyc_trips')
    env['POSTGRES_USER'] = os.getenv('POSTGRES_USER', 'postgres')
    env['POSTGRES_PASSWORD'] = os.getenv('POSTGRES_PASSWORD', 'postgres')

    print("=" * 60)
    print("DBT RUN - SILVER LAYER")
    print("=" * 60)

    result = subprocess.run(
        ["dbt", "run", "--select", "silver", "--profiles-dir", "."],
        cwd=dbt_path, env=env, capture_output=True, text=True
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt run failed: {result.stderr}")

    print("=" * 60)
    print("SILVER LAYER COMPLETADA")
    print("=" * 60)

    return {"status": "success"}


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success'
