import json
from pathlib import Path


TARGETS = [
    Path(r"F:/Deber2/data_analysis_20_questions.ipynb"),
    Path(r"F:/Deber2/mage_data/nyc_trips_pipeline/data_analysis_20_questions.ipynb"),
]


md_cell = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "## 5.1) Top 5 boroughs por mes (pickup) excluyendo `Unknown`\n",
        "Tablas usadas: `gold.fct_trips`, `gold.dim_zone`.\n",
        "Interpretacion: para lectura de negocio geografica, se excluye `Unknown`; este valor se considera un indicador de calidad de datos y puede analizarse por separado.",
    ],
}


code_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "run(\"\"\"\n"
        "WITH base AS (\n"
        "  SELECT DATE_TRUNC('month', f.pickup_date_key) AS month, z.borough, COUNT(*) AS trips\n"
        "  FROM gold.fct_trips f\n"
        "  JOIN gold.dim_zone z ON f.pu_zone_key = z.zone_key\n"
        "  WHERE f.pickup_date_key >= DATE '2024-01-01'\n"
        "    AND f.pickup_date_key < DATE '2025-01-01'\n"
        "    AND z.borough <> 'Unknown'\n"
        "  GROUP BY 1, 2\n"
        "), r AS (\n"
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY month ORDER BY trips DESC) AS rn\n"
        "  FROM base\n"
        ")\n"
        "SELECT month, borough, trips\n"
        "FROM r\n"
        "WHERE rn <= 5\n"
        "ORDER BY month, trips DESC;\n"
        "\"\"\")"
    ],
}


for path in TARGETS:
    if not path.exists():
        continue

    nb = json.loads(path.read_text(encoding="utf-8"))

    if any(
        c.get("cell_type") == "markdown"
        and "## 5.1) Top 5 boroughs por mes (pickup) excluyendo `Unknown`" in "".join(c.get("source", []))
        for c in nb.get("cells", [])
    ):
        continue

    insert_idx = None
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") == "markdown" and "## 5) Top 5 boroughs por mes (pickup)" in "".join(cell.get("source", [])):
            insert_idx = i + 2
            break

    if insert_idx is None:
        continue

    nb["cells"].insert(insert_idx, md_cell)
    nb["cells"].insert(insert_idx + 1, code_cell)
    path.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")

print("Pregunta 5.1 agregada en notebooks.")
