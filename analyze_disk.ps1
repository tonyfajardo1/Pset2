Write-Host "=== ANALISIS DE ESPACIO EN DISCO C: ===" -ForegroundColor Cyan
Write-Host ""

# Espacio total
$drive = Get-PSDrive C
Write-Host "Disco C: Usado: $([math]::Round($drive.Used/1GB,2)) GB | Libre: $([math]::Round($drive.Free/1GB,2)) GB" -ForegroundColor Yellow
Write-Host ""

# Carpetas principales a analizar
$folders = @(
    @{Path="C:\Users\Tony\AppData\Local"; Name="AppData\Local"},
    @{Path="C:\Users\Tony\AppData\Local\Docker"; Name="Docker (Local)"},
    @{Path="C:\Users\Tony\AppData\Local\Temp"; Name="Temp Usuario"},
    @{Path="C:\Users\Tony\Documents"; Name="Documentos"},
    @{Path="C:\Users\Tony\Downloads"; Name="Descargas"},
    @{Path="C:\Users\Tony\Desktop"; Name="Escritorio"},
    @{Path="C:\Users\Tony\Videos"; Name="Videos"},
    @{Path="C:\Users\Tony\Pictures"; Name="Imagenes"},
    @{Path="C:\Users\Tony\.docker"; Name=".docker"},
    @{Path="C:\ProgramData\Docker"; Name="ProgramData\Docker"},
    @{Path="C:\Windows\Temp"; Name="Windows Temp"},
    @{Path="C:\Program Files"; Name="Program Files"},
    @{Path="C:\Program Files (x86)"; Name="Program Files (x86)"}
)

Write-Host "=== TAMAÑO POR CARPETA ===" -ForegroundColor Cyan
foreach ($folder in $folders) {
    if (Test-Path $folder.Path) {
        $size = (Get-ChildItem $folder.Path -Recurse -Force -ErrorAction SilentlyContinue |
                 Measure-Object -Property Length -Sum -ErrorAction SilentlyContinue).Sum
        $sizeGB = [math]::Round($size / 1GB, 2)
        if ($sizeGB -gt 1) {
            Write-Host ("{0,10:N2} GB - {1}" -f $sizeGB, $folder.Name) -ForegroundColor Red
        } else {
            Write-Host ("{0,10:N2} GB - {1}" -f $sizeGB, $folder.Name)
        }
    }
}

Write-Host ""
Write-Host "=== ARCHIVOS MAS GRANDES EN USUARIO ===" -ForegroundColor Cyan
Get-ChildItem "C:\Users\Tony" -Recurse -File -Force -ErrorAction SilentlyContinue |
    Sort-Object Length -Descending |
    Select-Object -First 15 |
    ForEach-Object {
        $sizeGB = [math]::Round($_.Length / 1GB, 2)
        $sizeMB = [math]::Round($_.Length / 1MB, 2)
        if ($sizeGB -gt 0.1) {
            Write-Host ("{0,8:N2} GB - {1}" -f $sizeGB, $_.FullName)
        }
    }
