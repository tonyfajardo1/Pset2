import json
from pathlib import Path


path = Path(r"F:/Deber2/data_analysis_20_questions.ipynb")
nb = json.loads(path.read_text(encoding="utf-8"))

q = 0
for i, cell in enumerate(nb.get("cells", [])):
    source = "".join(cell.get("source", []))
    if cell.get("cell_type") == "markdown" and source.strip().startswith("## "):
        q += 1
        print(f"Q{q}: {source.strip().splitlines()[0]}")

    if cell.get("cell_type") == "code" and 'run("""' in source:
        outputs = cell.get("outputs", [])
        has_error = any(o.get("output_type") == "error" for o in outputs)
        print(
            f"  cell={i} exec={cell.get('execution_count')} outputs={len(outputs)} status={'ERROR' if has_error else 'OK'}"
        )
