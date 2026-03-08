"""
Bloque Custom: Ejecuta dbt para construir la capa Gold.

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
def build_gold_layer(*args, **kwargs):
    """
    Ejecuta particionamiento + dbt run --select gold
    """
    dbt_path = "/home/src/nyc_trips_pipeline/dbt_project"

    # Obtener credenciales de Mage Secrets
    env = os.environ.copy()
    env['POSTGRES_HOST'] = get_secret_value('POSTGRES_HOST') or 'postgres'
    env['POSTGRES_PORT'] = get_secret_value('POSTGRES_PORT') or '5432'
    env['POSTGRES_DB'] = get_secret_value('POSTGRES_DB') or 'nyc_trips'
    env['POSTGRES_USER'] = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER', '')
    env['POSTGRES_PASSWORD'] = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD', '')
    if not env['POSTGRES_USER'] or not env['POSTGRES_PASSWORD']:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')

    print("=" * 60)
    print("DBT BUILD GOLD")
    print("=" * 60)
    print(f"Host: {env['POSTGRES_HOST']}")
    print(f"Database: {env['POSTGRES_DB']}")
    print("=" * 60)

    # 1) Ejecutar script de particionamiento
    print("\n>>> Ejecutando scripts/create_partitions.sql")
    partitions_result = subprocess.run(
        ["bash", "-lc", "psql \"$DBT_POSTGRES_URL\" -f scripts/create_partitions.sql"],
        cwd=dbt_path, env={
            **env,
            'DBT_POSTGRES_URL': (
                f"postgresql://{env['POSTGRES_USER']}:{env['POSTGRES_PASSWORD']}"
                f"@{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/{env['POSTGRES_DB']}"
            )
        }, capture_output=True, text=True
    )
    print(partitions_result.stdout)
    if partitions_result.returncode != 0:
        raise Exception(f"partition script failed: {partitions_result.stderr}")

    # 2) dbt run --select gold
    print("\n>>> dbt run --select gold")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--select", "gold"],
        cwd=dbt_path, env=env, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run gold failed: {result.stderr}")

    print("\n" + "=" * 60)
    print("GOLD LAYER COMPLETADA")
    print("=" * 60)

    return {"status": "success", "layer": "gold"}


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success'
