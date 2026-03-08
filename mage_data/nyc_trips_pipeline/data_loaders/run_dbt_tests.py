import subprocess
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def run_dbt_tests(*args, **kwargs):
    """
    Ejecuta dbt test para validar la calidad de datos.

    Tests incluidos:
    - unique + not_null en keys
    - relationships entre tablas
    - accepted_values para dominios
    """
    dbt_project_path = "/home/src/dbt_project"

    print("=" * 50)
    print("Ejecutando dbt test...")
    print("=" * 50)

    test_result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=dbt_project_path,
        capture_output=True,
        text=True
    )

    print(test_result.stdout)
    if test_result.stderr:
        print("STDERR:", test_result.stderr)

    results = {
        'returncode': test_result.returncode,
        'success': test_result.returncode == 0,
        'output': test_result.stdout
    }

    if test_result.returncode != 0:
        print("\n" + "=" * 50)
        print("ALGUNOS TESTS FALLARON - Revisar output arriba")
        print("=" * 50)
        raise Exception(f"dbt test failed: {test_result.stderr}")
    else:
        print("\n" + "=" * 50)
        print("TODOS LOS TESTS PASARON EXITOSAMENTE")
        print("=" * 50)

    return results


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is None'
    assert output.get('success'), 'Some dbt tests failed'
