#!/bin/bash
set -e

mkdir -p /opt/hadoop/logs

export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export SPARK_HOME=/opt/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

wait_for_port() {
    local host="$1" port="$2"
    while ! timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; do
        echo "Waiting for $host:$port..."
        sleep 2
    done
}

# Đợi MinIO
wait_for_port minio 9000
echo "MinIO is up. Waiting 5s for full initialization..."
sleep 5

# =========================================================================
# TỰ ĐỘNG TẠO BUCKET BẰNG MINIO CLIENT (Khắc phục lỗi AWS Signature V4)
# =========================================================================
echo "Creating bucket 'lakehouse' (curl + SigV4, khong can mc vi dl.min.io da bi go)..."
# PUT bucket: 200 = tao moi, 409 = da ton tai (BucketAlreadyOwnedByYou) -> deu OK
code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
    --user "${MINIO_ACCESS_KEY:-minioadmin}:${MINIO_SECRET_KEY:-minioadmin}" \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    "http://minio:9000/${BUCKET_NAME:-lakehouse}")
if [ "$code" != "200" ] && [ "$code" != "409" ]; then
    echo "ERROR: tao bucket that bai (HTTP $code)"; exit 1
fi
echo "Bucket 'lakehouse' setup completed successfully."
# =========================================================================

# Khởi tạo Hive Metastore schema
echo "Checking Hive Metastore schema..."
if [ ! -d "/opt/hive/metastore_db/derby" ]; then
    echo "Initializing Hive Metastore schema..."
    $HIVE_HOME/bin/schematool -dbType derby -initSchema -verbose || true
else
    echo "Metastore schema already exists."
fi

# Khởi động Hive Metastore
echo "Starting Hive Metastore..."
nohup $HIVE_HOME/bin/hive --service metastore > /opt/hadoop/logs/hive-metastore.log 2>&1 &
wait_for_port localhost 9083

# Khởi động Spark Thrift Server
echo "Starting Spark Thrift Server..."
$SPARK_HOME/sbin/start-thriftserver.sh \
    --master local[*] \
    --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
    --conf spark.sql.warehouse.dir=s3a://lakehouse/warehouse \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    --hiveconf hive.server2.thrift.port=10001 \
    --hiveconf hive.server2.authentication=NOSASL

wait_for_port localhost 10001
echo "Lakehouse services are ready!"

tail -f /dev/null