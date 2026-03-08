import subprocess
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

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
    dbt_project_path = "/home/src/nyc_trips_pipeline/dbt_project"

    env = os.environ.copy()
    env['POSTGRES_HOST'] = get_secret_value('POSTGRES_HOST') or os.getenv('POSTGRES_HOST', 'postgres')
    env['POSTGRES_PORT'] = str(get_secret_value('POSTGRES_PORT') or os.getenv('POSTGRES_PORT', '5432'))
    env['POSTGRES_DB'] = get_secret_value('POSTGRES_DB') or os.getenv('POSTGRES_DB', 'nyc_trips')
    env['POSTGRES_USER'] = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER', '')
    env['POSTGRES_PASSWORD'] = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD', '')

    if not env['POSTGRES_USER'] or not env['POSTGRES_PASSWORD']:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')

    results = {}

    # Paso 1: Crear particiones
    print("=" * 50)
    print("Ejecutando create_partitions.sql...")
    print("=" * 50)

    partition_result = subprocess.run(
        ["bash", "-lc", "psql \"$DBT_POSTGRES_URL\" -f scripts/create_partitions.sql"],
        cwd=dbt_project_path,
        env={
            **env,
            'DBT_POSTGRES_URL': (
                f"postgresql://{env['POSTGRES_USER']}:{env['POSTGRES_PASSWORD']}"
                f"@{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/{env['POSTGRES_DB']}"
            ),
        },
        capture_output=True,
        text=True,
    )

    print(partition_result.stdout)
    if partition_result.stderr:
        print("STDERR:", partition_result.stderr)

    if partition_result.returncode != 0:
        raise Exception(f"create_partitions failed: {partition_result.stderr}")

    # Paso 2: Ejecutar dbt run para Gold
    print("=" * 50)
    print("Ejecutando dbt run --select gold...")
    print("=" * 50)

    run_result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--select", "gold"],
        cwd=dbt_project_path,
        env=env,
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

    # Paso 3: Validar que fct_trips no quede vacia
    print("\n" + "=" * 50)
    print("Validando contenido de gold.fct_trips...")
    print("=" * 50)

    validate_result = subprocess.run(
        [
            "bash",
            "-lc",
            (
                "psql \"$DBT_POSTGRES_URL\" -t -A -c "
                "\"SELECT COUNT(*) FROM gold.fct_trips;\""
            ),
        ],
        cwd=dbt_project_path,
        env={
            **env,
            'DBT_POSTGRES_URL': (
                f"postgresql://{env['POSTGRES_USER']}:{env['POSTGRES_PASSWORD']}"
                f"@{env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}/{env['POSTGRES_DB']}"
            ),
        },
        capture_output=True,
        text=True,
    )

    if validate_result.returncode != 0:
        raise Exception(f"No se pudo validar gold.fct_trips: {validate_result.stderr}")

    rows = int((validate_result.stdout or '0').strip() or 0)
    print(f"Rows en gold.fct_trips: {rows}")

    if rows == 0:
        raise Exception("gold.fct_trips quedo vacia despues de dbt run --select gold")

    print("\n" + "=" * 50)
    print("GOLD LAYER COMPLETADA EXITOSAMENTE")
    print("=" * 50)

    return results


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is None'
    assert output.get('run_gold', {}).get('success'), 'dbt run gold failed'
