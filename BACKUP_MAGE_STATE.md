# Backup de estado de Mage (triggers + runs + logs)

Para conservar el estado interno de Mage entre PCs (incluyendo triggers creados por UI), respalda la carpeta:

- `mage_data/mage_data/`

En esta ruta vive la base interna de Mage (`mage-ai.db`) y metadatos de ejecucion.

## Recomendado

1. Detener contenedor de Mage.
2. Crear backup.
3. Volver a levantar Mage.

## Backup rapido (PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\backup_mage_state.ps1
```

## Restaurar en otra PC

1. Clonar el repo.
2. Copiar el backup en la nueva maquina.
3. Restaurar con:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\restore_mage_state.ps1 -BackupPath "RUTA_DEL_BACKUP"
```

4. Levantar servicios:

```bash
docker compose up -d
```

## Nota

- Este backup es solo para estado local (scheduler/runs/logs de Mage).
- No subir `mage_data/mage_data/` al repo (ya esta ignorado en `.gitignore`).
