"""Offline source/data checks; run with Python 3.10+ from the project root."""
import argparse
import ast
import csv
import json
import math
import tarfile
import tempfile
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "notebooks" if (ROOT / "notebooks").is_dir() else ROOT
PIPELINE_DIR = ROOT / "pipeline" if (ROOT / "pipeline").is_dir() else ROOT.parent / "pipeline"
STEPS_PATH = PIPELINE_DIR / "steps.py"
PIPELINE_NOTEBOOKS = (
    "01_ingest_bronze.ipynb",
    "02_transform_silver.ipynb",
    "03_dim_courier_silver.ipynb",
    "04_fact_and_ml_gold.ipynb",
    "04b_eta_safe.ipynb",
    "04c_eta_paper.ipynb",
    "05_serving_layer.ipynb",
)


def render_notebook_steps() -> str:
    blocks = [
        "# Generated from notebook code cells tagged pipeline-step.\n"
        "# Run python scripts/check_project.py --sync-steps after editing those cells."
    ]
    for name in PIPELINE_NOTEBOOKS:
        notebook = json.loads((NOTEBOOK_DIR / name).read_text(encoding="utf-8"))
        sources = [
            "".join(cell["source"]).rstrip()
            for cell in notebook["cells"]
            if cell["cell_type"] == "code" and "pipeline-step" in cell.get("metadata", {}).get("tags", [])
        ]
        if not sources or any(not source.strip() for source in sources):
            raise ValueError(f"Missing pipeline-step code in {name}")
        blocks.append(f"# notebooks/{name}\n" + "\n\n\n".join(sources))
    generated = "\n\n\n".join(blocks) + "\n"
    compile(generated, str(STEPS_PATH), "exec")
    return generated


def check_notebook_steps() -> None:
    if not STEPS_PATH.is_file() or STEPS_PATH.read_text(encoding="utf-8") != render_notebook_steps():
        raise ValueError(
            "pipeline/steps.py differs from the notebooks. "
            "Run python scripts/check_project.py --sync-steps"
        )


def sync_notebook_steps() -> None:
    source = render_notebook_steps()
    if STEPS_PATH.is_file() and STEPS_PATH.read_text(encoding="utf-8") == source:
        return
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=PIPELINE_DIR,
        prefix="steps-", suffix=".tmp", delete=False,
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(source)
        mode = STEPS_PATH.stat().st_mode & 0o777 if STEPS_PATH.exists() else 0o644
        temporary.chmod(mode)
        temporary.replace(STEPS_PATH)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync-steps", action="store_true", help="Regenerate pipeline/steps.py from the notebooks")
    args = parser.parse_args()
    if args.sync_steps:
        sync_notebook_steps()
        print("Notebook steps are synced to pipeline/steps.py")
        return
    check_notebook_steps()
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
