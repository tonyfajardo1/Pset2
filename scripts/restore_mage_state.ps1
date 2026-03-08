param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath
)

$ErrorActionPreference = "Stop"

$projectRoot = "F:\Deber2"
$targetPath = Join-Path $projectRoot "mage_data\mage_data"

if (-not (Test-Path $BackupPath)) {
    throw "No existe el backup: $BackupPath"
}

$sourcePath = Join-Path $BackupPath "mage_data"
if (-not (Test-Path $sourcePath)) {
    throw "Backup invalido: falta carpeta mage_data dentro de $BackupPath"
}

Write-Host "[1/4] Deteniendo nyc_mage ..."
docker stop nyc_mage | Out-Null

Write-Host "[2/4] Limpiando estado actual ..."
if (Test-Path $targetPath) {
    Remove-Item -Path $targetPath -Recurse -Force
}

Write-Host "[3/4] Restaurando estado desde backup ..."
Copy-Item -Path $sourcePath -Destination $targetPath -Recurse -Force

Write-Host "[4/4] Iniciando nyc_mage ..."
docker start nyc_mage | Out-Null

Write-Host "Restauracion completada desde: $BackupPath"
