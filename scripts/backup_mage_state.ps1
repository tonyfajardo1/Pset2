param(
    [string]$BackupRoot = "F:\Deber2\backups"
)

$ErrorActionPreference = "Stop"

$projectRoot = "F:\Deber2"
$mageStatePath = Join-Path $projectRoot "mage_data\mage_data"

if (-not (Test-Path $mageStatePath)) {
    throw "No se encontro $mageStatePath. Asegurate de haber levantado Mage al menos una vez."
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupDir = Join-Path $BackupRoot "mage-state-$timestamp"

New-Item -ItemType Directory -Path $backupDir -Force | Out-Null

Write-Host "[1/3] Deteniendo nyc_mage para backup consistente..."
docker stop nyc_mage | Out-Null

Write-Host "[2/3] Copiando estado de Mage a $backupDir ..."
Copy-Item -Path $mageStatePath -Destination $backupDir -Recurse -Force

Write-Host "[3/3] Volviendo a levantar nyc_mage ..."
docker start nyc_mage | Out-Null

Write-Host "Backup completado: $backupDir"
