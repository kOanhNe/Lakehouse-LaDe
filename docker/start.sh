#!/bin/bash
set -euo pipefail
mkdir -p /opt/hadoop/logs

wait_for_port() {
    local host="$1" port="$2" attempt
    for attempt in $(seq 1 90); do
        if timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
    echo "Timed out waiting for $host:$port; inspect /opt/hadoop/logs" >&2
    return 1
}

wait_for_port minio 9000

# Validate the schema; directory existence alone does not prove initialization.
if [ ! -f /opt/hive/metastore_db/derby/service.properties ]; then
    "$HIVE_HOME/bin/schematool" -dbType derby -initSchema -verbose
fi
"$HIVE_HOME/bin/schematool" -dbType derby -info
"$HIVE_HOME/bin/hive" --service metastore > /opt/hadoop/logs/hive-metastore.log 2>&1 &
metastore_pid=$!
trap 'kill "$metastore_pid" 2>/dev/null || true; "$SPARK_HOME/sbin/stop-thriftserver.sh" || true' EXIT
trap 'exit 0' TERM INT
wait_for_port localhost 9083
"$SPARK_HOME/sbin/start-thriftserver.sh" \
    --master 'local[2]' \
    --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
    --conf spark.sql.warehouse.dir=s3a://lakehouse/warehouse \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    --hiveconf hive.server2.thrift.port=10001 \
    --hiveconf hive.server2.authentication=NOSASL
wait_for_port localhost 10001
echo "Lakehouse services are ready"
while kill -0 "$metastore_pid" 2>/dev/null; do
    timeout 2 bash -c 'echo > /dev/tcp/localhost/10001' || exit 1
    sleep 5 &
    wait $!
done
exit 1
