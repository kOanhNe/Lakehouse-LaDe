"""Persist flow attempts and serialize writers to the shared warehouse."""

from contextlib import contextmanager
import logging
from uuid import uuid4


TABLE = "lakehouse_pipeline_runs"
LOCK = "lakehouse.delivery-pipeline"
logger = logging.getLogger(__name__)


def connect(spark, jdbc):
    jvm = spark.sparkContext._jvm
    properties = jvm.java.util.Properties()
    for key in ("user", "password"):
        properties.setProperty(key, jdbc[key])
    connection = jvm.com.mysql.cj.jdbc.Driver().connect(jdbc["url"], properties)
    connection.setAutoCommit(True)
    return connection


def execute(connection, sql, *values):
    statement = connection.prepareStatement(sql)
    try:
        for index, value in enumerate(values, start=1):
            statement.setString(index, value)
        return statement.executeUpdate()
    finally:
        statement.close()


def scalar(connection, sql, *values):
    statement = connection.prepareStatement(sql)
    try:
        for index, value in enumerate(values, start=1):
            statement.setString(index, value)
        result = statement.executeQuery()
        try:
            if result.next():
                return result.getString(1)
        finally:
            result.close()
    finally:
        statement.close()


class PipelineRun:
    def __init__(self, connection, run_id):
        self.connection = connection
        self.run_id = run_id

    def _require_lock(self):
        if scalar(self.connection, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", LOCK) != "1":
            raise RuntimeError("Pipeline write lock was lost")

    def stage(self, stage):
        self._require_lock()
        execute(self.connection, f"UPDATE {TABLE} SET stage = ? WHERE run_id = ?", stage, self.run_id)

    def finish(self, status, error_type=None):
        self._require_lock()
        changed = execute(
            self.connection,
            f"""UPDATE {TABLE} SET status = ?, ended_at = UTC_TIMESTAMP(6), error_type = ?
                WHERE run_id = ? AND status = 'RUNNING'""",
            status, error_type, self.run_id,
        )
        if changed != 1:
            raise RuntimeError(f"Could not mark running attempt {self.run_id} as {status}")


@contextmanager
def track_run(spark, jdbc, prefect_run_id):
    connection = connect(spark, jdbc)
    try:
        if scalar(connection, "SELECT GET_LOCK(?, 0)", LOCK) != "1":
            raise RuntimeError("Another delivery pipeline run already owns the write lock")
        execute(connection, f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                seq BIGINT AUTO_INCREMENT PRIMARY KEY,
                run_id CHAR(32) NOT NULL UNIQUE,
                prefect_run_id CHAR(36) NOT NULL,
                owner_connection BIGINT NOT NULL,
                status VARCHAR(16) NOT NULL,
                stage VARCHAR(32) NOT NULL,
                started_at DATETIME(6) NOT NULL,
                ended_at DATETIME(6),
                error_type VARCHAR(64)
            ) ENGINE=InnoDB
        """)
        execute(connection, f"""UPDATE {TABLE} SET status = 'INTERRUPTED', ended_at = UTC_TIMESTAMP(6)
                                 WHERE status = 'RUNNING'""")
        run = PipelineRun(connection, uuid4().hex)
        execute(connection, f"""
            INSERT INTO {TABLE}
                (run_id, prefect_run_id, owner_connection, status, stage, started_at)
            VALUES (?, ?, CONNECTION_ID(), 'RUNNING', 'starting', UTC_TIMESTAMP(6))
        """, run.run_id, prefect_run_id)
        try:
            yield run
            run.finish("COMPLETED")
        except BaseException as error:
            try:
                run.finish("FAILED", type(error).__name__)
            except Exception:
                logger.exception("Could not record failure for pipeline run %s", run.run_id)
            raise
    finally:
        # MySQL releases the advisory lock when this dedicated connection closes.
        connection.close()
