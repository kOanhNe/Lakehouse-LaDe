FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
RUN pip install --no-cache-dir --no-deps delta-spark==3.3.2
USER ${NB_UID}
