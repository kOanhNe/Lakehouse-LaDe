FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
RUN pip install --no-cache-dir --no-deps delta-spark==3.3.2
USER ${NB_UID}
RUN pip install --no-cache-dir "prefect>=3.0,<4" "PyYAML>=6.0,<7" "lightgbm>=4.0,<5" "scikit-learn>=1.4,<2"
