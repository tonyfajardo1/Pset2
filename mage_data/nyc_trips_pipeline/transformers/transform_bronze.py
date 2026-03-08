if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def pass_files(files_list, *args, **kwargs):
    """
    Pasa la lista de archivos al exporter.
    La verificacion de idempotencia se hace en el exporter.
    """
    print(f"Archivos a procesar: {len(files_list)}")
    for f in files_list:
        print(f"  - {f['filename']}")
    return files_list
