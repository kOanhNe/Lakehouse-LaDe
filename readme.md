# Delivery Lakehouse — LaDe-D Yantai

This project ingests the Yantai delivery extract from LaDe-D into Delta Lake, profiles its quality, builds a courier history and serves aggregate tables to MySQL/Metabase.

The important data decision is documented before the pipeline: the source contains 206,431 orders, one city, 205 couriers, 17 regions, 8,163 AOIs and 16 AOI codes. `delivery_minutes` means accept-to-completion time. It is not driving time. GPS fields are event-nearest observations, so GPS availability is tracked explicitly.

Read the data findings first:

- [Data audit report](reports/data/README.md)
- [Data dictionary](docs/data_dictionary.md)
- [Generated summary](reports/data/summary.json)
- [Notebook run order](notebooks/README.md)

## Run

```powershell
docker compose up -d --build
```

Open Jupyter at `http://localhost:8888`. Run notebooks `00` through `07` in the order in [notebooks/README.md](notebooks/README.md). The source file must be uploaded to MinIO as `s3a://lakehouse/delivery_yt.csv`.

ETA is evaluated at query time: each sample represents a package still pending at a courier query point, and the target is remaining minutes until delivery. Notebook 06 writes `gold/ml/eta_query_samples`; notebook 07 evaluates a global median, leakage-safe historical SPEED baseline and Spark GBT on `log1p(target_remaining_minutes)`, then writes `gold/ml/eta_eval_by_cohort`. This query-time task is different from the LaDe paper's original delivery-time experiment, so its results must not be compared directly with Table 4.

The local audit can run without Spark:

```powershell
.venv/Scripts/python.exe scripts/profile_delivery.py
```

It writes a row-level quality audit and tables under `reports/data`. Silver keeps positive-duration orders and records quality flags for cross-day orders, >24-hour durations, invalid GPS and large straight-line distances. Only the explicit analytical cohort in each Gold/ML notebook may remove a flagged cohort.

The Compose services are MinIO (`9001`), Jupyter (`8888`), MySQL (`3306`) and Metabase (`3000`). MySQL, Metabase, MinIO and the Hive metastore use named volumes. Use `docker compose down -v` only when a full reset is intended.

## Checks

```powershell
.venv/Scripts/python.exe scripts/check_project.py
docker run --rm --mount type=bind,source=${PWD},target=/workspace,readonly --workdir /workspace --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.3-python3 /workspace/scripts/test_spark.py
```

The second command checks the SCD2 tie policy, one-to-one fact grain, missing/overlapping history and GPS distance validity.

## Sources

The official LaDe-D data card, repository README, paper and website snapshot are stored under `reports/data/sources`. The source split is `delivery_yt`; its official metadata reports 206,431 examples.
