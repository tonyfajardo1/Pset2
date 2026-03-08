import json
from pathlib import Path


NOTEBOOKS = [
    Path(r"F:/Deber2/data_analysis_20_questions.ipynb"),
    Path(r"F:/Deber2/mage_data/nyc_trips_pipeline/data_analysis_20_questions.ipynb"),
]


INTERPRETATIONS = {
    "## 1)": "Interpretacion: La demanda presenta estacionalidad en 2024, con meses de mayor y menor volumen. Esto sugiere variaciones por clima, turismo y dinamica urbana.",
    "## 2)": "Interpretacion: El servicio yellow concentra mas viajes que green en todos los meses, aunque ambos siguen una estacionalidad parecida. La diferencia principal es de escala.",
    "## 3)": "Interpretacion: Los pickups se concentran en hubs de alta demanda (aeropuertos y zonas centrales). Esto confirma una centralizacion geografica del origen de viajes.",
    "## 4)": "Interpretacion: Los dropoffs mas frecuentes tambien se ubican en zonas de alta actividad. El patron de destinos es consistente con los origenes mas demandados.",
    "## 5)": "Interpretacion: Manhattan domina de forma recurrente y Queens destaca por flujo aeroportuario. La composicion mensual por borough es estable en estructura general.",
    "## 6)": "Interpretacion: Se observan horas pico por dia de semana, utiles para planificar oferta operativa. El patron refleja commuting y actividad de ocio.",
    "## 7)": "Interpretacion: Hay dias sistematicamente mas activos que otros, lo que evidencia un ciclo semanal claro. Este ranking sirve para ajustar capacidad por dia.",
    "## 8)": "Interpretacion: El ingreso mensual sigue de cerca el volumen de viajes. Los meses con mayor demanda tienden a mayor recaudacion total.",
    "## 9)": "Interpretacion: Yellow aporta la mayor parte del ingreso en cada mes, consistente con su mayor cantidad de viajes. Green complementa en menor escala.",
    "## 10)": "Interpretacion: El tip porcentual promedio cambia por mes, lo que sugiere variaciones de comportamiento de pago y contexto operativo.",
    "## 11)": "Interpretacion: Existen diferencias de tip porcentual entre boroughs, lo que indica perfiles de demanda y patrones de pago distintos por zona.",
    "## 12)": "Interpretacion: Las zonas con mayor ingreso total se alinean con areas de alto flujo. No siempre coinciden exactamente con las de mayor volumen, por ticket promedio.",
    "## 13)": "Interpretacion: Con un minimo de viajes, el ranking de tip% es mas robusto y menos sensible a outliers. Se identifican zonas con propina alta de forma consistente.",
    "## 14)": "Interpretacion: Card y cash muestran diferencias en volumen, ingreso y propina. En general, card tiende a mejor tip porcentual promedio.",
    "## 15)": "Interpretacion: La duracion media de viaje varia por mes, posiblemente por congestion, clima y mezcla de rutas.",
    "## 16)": "Interpretacion: La distancia promedio mensual cambia con la demanda y tipo de trayectos predominantes. Complementa el analisis de duracion.",
    "## 17)": "Interpretacion: La velocidad promedio difiere por borough y franja horaria, reflejando efectos de trafico y densidad urbana.",
    "## 18)": "Interpretacion: El p50 representa el viaje tipico y el p90 muestra la cola de viajes largos. La brecha entre ambos indica dispersion de duraciones.",
    "## 19)": "Interpretacion: Las zonas con mayor p90 concentran viajes extremos en tiempo. Son candidatas para monitoreo de congestion y planificacion operativa.",
    "## 20)": "Interpretacion: Las rutas borough->borough mas frecuentes revelan los principales corredores de movilidad. Sirven para priorizar analisis y estrategias de servicio.",
}


for notebook in NOTEBOOKS:
    if not notebook.exists():
        continue

    data = json.loads(notebook.read_text(encoding="utf-8"))

    for cell in data.get("cells", []):
        if cell.get("cell_type") != "markdown":
            continue

        source = cell.get("source", [])
        if not source:
            continue

        first_line = source[0]
        if not first_line.startswith("## "):
            continue

        key = next((k for k in INTERPRETATIONS.keys() if first_line.startswith(k)), None)
        if not key:
            continue

        new_lines = [source[0]]

        tables_line = next((line for line in source if line.startswith("Tablas usadas:")), None)
        if tables_line:
            new_lines.append(tables_line)

        new_lines.append(INTERPRETATIONS[key])
        cell["source"] = new_lines

    notebook.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")

print("Interpretaciones actualizadas en ambos notebooks.")
