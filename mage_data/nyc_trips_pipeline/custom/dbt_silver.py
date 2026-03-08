"""
Bloque Custom: Ejecuta dbt para construir la capa Silver.

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
def build_silver_layer(*args, **kwargs):
    """
    Ejecuta dbt seed + dbt run --select silver
    """
    dbt_path = "/home/src/dbt_project"

    # Obtener credenciales de Mage Secrets
    env = os.environ.copy()
    env['POSTGRES_HOST'] = get_secret_value('POSTGRES_HOST') or 'postgres'
    env['POSTGRES_PORT'] = get_secret_value('POSTGRES_PORT') or '5432'
    env['POSTGRES_DB'] = get_secret_value('POSTGRES_DB') or 'nyc_trips'
    env['POSTGRES_USER'] = get_secret_value('POSTGRES_USER') or 'postgres'
    env['POSTGRES_PASSWORD'] = get_secret_value('POSTGRES_PASSWORD') or 'postgres'

    print("=" * 60)
    print("DBT BUILD SILVER")
    print("=" * 60)
    print(f"Host: {env['POSTGRES_HOST']}")
    print(f"Database: {env['POSTGRES_DB']}")
    print("=" * 60)

    # 1. dbt seed (cargar taxi_zones)
    print("\n>>> Paso 1: dbt seed")
    r1 = subprocess.run(
        ["dbt", "seed", "--profiles-dir", "."],
        cwd=dbt_path, env=env, capture_output=True, text=True
    )
    print(r1.stdout)
    if r1.returncode != 0:
        raise Exception(f"dbt seed failed: {r1.stderr}")

    # 2. dbt run --select silver
    print("\n>>> Paso 2: dbt run --select silver")
    r2 = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--select", "silver"],
        cwd=dbt_path, env=env, capture_output=True, text=True
    )
    print(r2.stdout)
    if r2.returncode != 0:
        raise Exception(f"dbt run failed: {r2.stderr}")

    print("\n" + "=" * 60)
    print("SILVER LAYER COMPLETADA")
    print("=" * 60)

    return {"status": "success", "layer": "silver"}


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success'
