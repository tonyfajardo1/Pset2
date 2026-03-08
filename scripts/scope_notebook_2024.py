import json
from pathlib import Path


TARGETS = [
    Path(r"F:/Deber2/data_analysis_20_questions.ipynb"),
    Path(r"F:/Deber2/mage_data/nyc_trips_pipeline/data_analysis_20_questions.ipynb"),
]


def apply_2024_scope(sql: str) -> str:
    if "EXPLAIN" in sql:
        return sql

    if "FROM gold.fct_trips" not in sql:
        return sql

    if "pickup_date_key >=" in sql:
        return sql

    col = "f.pickup_date_key" if "FROM gold.fct_trips f" in sql else "pickup_date_key"
    date_filter = (
        f"{col} >= DATE '2024-01-01'\n"
        f"  AND {col} < DATE '2025-01-01'"
    )

    if "WHERE" in sql:
        return sql.replace("WHERE ", f"WHERE {date_filter}\n  AND ", 1)

    for token in ["\nGROUP BY", "\nORDER BY", "\nLIMIT"]:
        if token in sql:
            return sql.replace(token, f"\nWHERE {date_filter}{token}", 1)

    return sql


for path in TARGETS:
    if not path.exists():
        continue

    nb = json.loads(path.read_text(encoding="utf-8"))

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        if cell.get("execution_count") is not None:
            continue

        source = "".join(cell.get("source", []))
        if "run(\"\"\"" not in source:
            continue

        start = source.find('run("""')
        end = source.rfind('""")')
        if start == -1 or end == -1 or end <= start:
            continue

        sql = source[start + 7:end]
        new_sql = apply_2024_scope(sql)

        if new_sql != sql:
            cell["source"] = [f'run("""{new_sql}""")']

    path.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")

print("Notebook scope updated to 2024 for unexecuted queries.")
