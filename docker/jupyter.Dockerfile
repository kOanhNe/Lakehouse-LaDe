FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3
USER root
RUN pip install --no-cache-dir --no-deps delta-spark==3.3.2
RUN install -d -o "${NB_UID}" -g "${NB_GID}" /home/jovyan/work
WORKDIR /home/jovyan/work
COPY --chown=${NB_UID}:${NB_GID} pyproject.toml requirements.txt ./
COPY --chown=${NB_UID}:${NB_GID} src/ ./src/
COPY --chown=${NB_UID}:${NB_GID} scripts/ ./scripts/
COPY --chown=${NB_UID}:${NB_GID} pipeline/ ./pipeline/
COPY --chown=${NB_UID}:${NB_GID} dashboard/ ./dashboard/
USER ${NB_UID}
RUN pip install --no-cache-dir -e . "prefect>=3.0,<4"
