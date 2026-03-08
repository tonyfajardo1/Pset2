"""
Bloque Custom: Ejecuta dbt test para validar calidad de datos.

Usa Mage Secrets para las credenciales de PostgreSQL.
"""
from mage_ai.data_preparation.shared.secrets import get_secret_value
import subprocess
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def run_quality_checks(*args, **kwargs):
    """
    Ejecuta dbt test para validar calidad de datos
    """
    dbt_path = "/home/src/nyc_trips_pipeline/dbt_project"

    # Obtener credenciales de Mage Secrets
    env = os.environ.copy()
    env['POSTGRES_HOST'] = get_secret_value('POSTGRES_HOST') or 'postgres'
    env['POSTGRES_PORT'] = get_secret_value('POSTGRES_PORT') or '5432'
    env['POSTGRES_DB'] = get_secret_value('POSTGRES_DB') or 'nyc_trips'
    env['POSTGRES_USER'] = get_secret_value('POSTGRES_USER') or 'postgres'
    env['POSTGRES_PASSWORD'] = get_secret_value('POSTGRES_PASSWORD') or 'postgres'

    print("=" * 60)
    print("DBT TEST - QUALITY CHECKS")
    print("=" * 60)

    # dbt test
    print("\n>>> dbt test")
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=dbt_path, env=env, capture_output=True, text=True
    )
    print(result.stdout)

    if result.returncode != 0:
        print("\n[ERROR] Algunos tests fallaron")
        print(result.stderr)
        raise Exception(f"dbt test failed: {result.stderr}")

    print("\n" + "=" * 60)
    print("TODOS LOS TESTS PASARON")
    print("=" * 60)

    return {"status": "success", "output": result.stdout}


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success', 'Some dbt tests failed'
