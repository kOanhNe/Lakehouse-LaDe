"""Load pipeline definitions from notebooks and check project inputs offline."""
import ast
import csv
import json
import math
import os
import sys
import tarfile
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import CodeType, ModuleType
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NOTEBOOK_DIR = ROOT / "notebooks" if (ROOT / "notebooks").is_dir() else ROOT
NOTEBOOK_DIR = Path(os.getenv("NOTEBOOK_DIR", str(DEFAULT_NOTEBOOK_DIR)))
PIPELINE_NOTEBOOKS = (
    "01_ingest_bronze.ipynb",
    "02_transform_silver.ipynb",
    "03_dim_courier_silver.ipynb",
    "04_fact_and_ml_gold.ipynb",
    "04b_eta_safe.ipynb",
    "04c_eta_paper.ipynb",
    "05_serving_layer.ipynb",
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
    helper_dir = str(NOTEBOOK_DIR.resolve())
    if helper_dir not in sys.path:
        sys.path.insert(0, helper_dir)
    steps = ModuleType("lakehouse_notebook_steps")
    for code in compiled:
        exec(code, steps.__dict__)
    return steps


def main():
    compile_notebook_steps()
    cells = 0
    for path in sorted((ROOT / 'notebooks').glob('*.ipynb')):
        notebook = json.loads(path.read_text(encoding='utf-8'))
        assert notebook['nbformat'] == 4, path
        for i, cell in enumerate(notebook['cells']):
            if cell['cell_type'] == 'code':
                ast.parse(''.join(cell['source']), filename=f'{path.name}:cell{i}')
                cells += 1
        print(f'Notebook OK: {path.name}')
    for path in (ROOT / 'notebooks').glob('*.py'):
        ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    for path in (ROOT / 'docker/config').glob('*.xml'):
        ElementTree.parse(path)
    for path in (ROOT / 'docker/jars').glob('*.jar'):
        with zipfile.ZipFile(path) as jar:
            assert jar.testzip() is None, path
    stats = Counter()
    identifiers = set()
    types = Counter()
    with (ROOT / 'data/delivery_yt.csv').open(encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        assert len(reader.fieldnames) == 17
        for row in reader:
            stats['rows'] += 1
            assert None not in row, f'Extra CSV fields at row {stats["rows"]}'
            key = row['order_id'].strip()
            assert key and key not in identifiers, f'Missing/duplicate order_id: {key}'
            identifiers.add(key)
            types[row['aoi_type']] += 1
            try:
                start = datetime.strptime('2022-' + row['accept_time'], '%Y-%m-%d %H:%M:%S')
                end = datetime.strptime('2022-' + row['delivery_time'], '%Y-%m-%d %H:%M:%S')
                duration = (end-start).total_seconds()/60
                stats['nonpositive_duration'] += duration <= 0
                stats['duration_gt24h'] += duration > 1440
                stats['train_rows'] += start < datetime(2022, 10, 1)
                stats['test_rows'] += start >= datetime(2022, 10, 1)
            except ValueError:
                stats['bad_timestamp'] += 1
            for prefix in ['', 'accept_gps_', 'delivery_gps_']:
                try:
                    lat, lng = float(row[prefix+'lat']), float(row[prefix+'lng'])
                    valid = math.isfinite(lat) and math.isfinite(lng) and -90 <= lat <= 90 and -180 <= lng <= 180 and (lat, lng) != (0, 0)
                except ValueError:
                    valid = False
                stats[(prefix or 'receiver_')+'invalid_coordinates'] += not valid
    print(f'Parsed {cells} code cells; XML/JAR integrity OK')
    print('CSV:', json.dumps(dict(stats), sort_keys=True))
    print('Observed AOI codes:', sorted(types))
    # Inspect distributions without unpacking or modifying binary inputs.
    for path in sorted(ROOT.glob('*.tar.gz')) + sorted(ROOT.glob('*.tgz')):
        with tarfile.open(path, 'r:gz') as archive:
            members = [m.name for m in archive if m.isfile() and ('hadoop-client-runtime-' in m.name or 'hadoop-aws-' in m.name or 'aws-java-sdk-bundle-' in m.name)]
        print(path.name, members)


if __name__ == '__main__':
    main()
