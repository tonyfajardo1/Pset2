"""
Custom Block: Ejecuta dbt run para la capa Silver.
"""
import subprocess
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

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
    env['POSTGRES_USER'] = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER', '')
    env['POSTGRES_PASSWORD'] = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD', '')
    if not env['POSTGRES_USER'] or not env['POSTGRES_PASSWORD']:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')

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
