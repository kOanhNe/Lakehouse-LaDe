# Notebook order

Run these notebooks in order after starting Compose:

1. `00_data_audit.ipynb` — reads the full-file audit in `reports/data` and explains the observed data before any filtering.
2. `01_ingest_bronze.ipynb` — copies the source into Delta Bronze.
3. `02_transform_silver.ipynb` — casts types, derives event fields, keeps positive-duration rows, and writes quality flags plus rejected non-positive rows.
4. `03_dim_courier_silver.ipynb` — builds daily dominant-region history with deterministic tie handling.
5. `04_fact_and_ml_gold.ipynb` — performs a one-to-one point-in-time join and evaluates ETA with a chronological split.
6. `05_serving_layer.ipynb` — publishes aggregates and ETA evaluation to MySQL.
7. `06_eta_query_dataset.ipynb` — builds leakage-safe query-time ETA samples.
8. `07_eta_model.ipynb` — evaluates global median, SPEED and GBT baselines.
