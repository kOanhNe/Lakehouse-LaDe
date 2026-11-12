"""Offline source/data checks; run with Python 3.10+ from the project root."""
import ast
import csv
import json
import math
import tarfile
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]


def main():
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
