# Notebook order

All notebooks are thin wrappers around the functions used by `pipeline/flow.py`; the pipeline and notebooks therefore produce the same outputs.

1. `00_data_audit.ipynb` - full raw-file audit before any Silver policy.
2. `01_ingest_bronze.ipynb` - write Bronze Delta.
3. `02_transform_silver.ipynb` - write Silver and rejected rows.
4. `03_dim_courier_silver.ipynb` - write the courier SCD2 dimension.
5. `04_fact_and_ml_gold.ipynb` - write fact Gold and run the secondary baseline.
6. `04b_eta_safe.ipynb` - run the primary leakage-safe ETA protocol.
7. `05_serving_layer.ipynb` - publish serving tables to MySQL.
