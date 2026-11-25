"""Load pipeline definitions directly from the stage notebooks."""
import ast
import json
import os
from pathlib import Path
from types import CodeType, ModuleType

from lakehouse import PROJECT_ROOT

NOTEBOOK_DIR = Path(os.getenv("NOTEBOOK_DIR", str(PROJECT_ROOT / "notebooks")))
PIPELINE_NOTEBOOKS = (
    "01_ingest_bronze.ipynb",
    "02_transform_silver.ipynb",
    "03_dim_courier_silver.ipynb",
    "04_fact.ipynb",
    "06_eta_accept.ipynb",
    "07_eta_paper.ipynb",
    "05_serving.ipynb",
)


def compile_notebook_steps() -> list[CodeType]:
    """Compile the tagged definitions before a flow can start executing tasks."""
    compiled = []
    for name in PIPELINE_NOTEBOOKS:
        path = NOTEBOOK_DIR / name
        notebook = json.loads(path.read_text(encoding="utf-8"))
        cells = [
            cell for cell in notebook["cells"]
            if cell["cell_type"] == "code" and "pipeline-step" in cell.get("metadata", {}).get("tags", [])
        ]
        if not cells:
            raise ValueError(f"Missing pipeline-step code in {name}")
        for index, cell in enumerate(cells, 1):
            location = f"{path}:{cell.get('id', index)}"
            tree = ast.parse("".join(cell["source"]), filename=location)
            if not tree.body:
                raise ValueError(f"Empty pipeline-step cell at {location}")
            for statement in tree.body:
                if isinstance(statement, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
                    continue
                if isinstance(statement, ast.Assign) and not any(
                    isinstance(node, ast.Call) for node in ast.walk(statement.value)
                ):
                    continue
                if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
                    continue
                raise ValueError(f"Move runnable examples out of the pipeline-step cell at {location}")
            compiled.append(compile(tree, location, "exec"))
    return compiled


def load_notebook_steps() -> ModuleType:
    """Load only notebook definitions into a shared namespace in memory."""
    compiled = compile_notebook_steps()
    steps = ModuleType("lakehouse_notebook_steps")
    for code in compiled:
        exec(code, steps.__dict__)
    return steps
