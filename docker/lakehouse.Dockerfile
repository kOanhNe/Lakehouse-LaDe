
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# =========================================================
# 1. BASIC PACKAGES + JAVA 11
# =========================================================
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    vim \
    wget \
    curl \
    net-tools \
    netcat-openbsd \
    iputils-ping \
    ca-certificates \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# =========================================================
# 2. HADOOP 3.3.6
# =========================================================
ADD hadoop-3.3.6.tar.gz /opt/

RUN mv /opt/hadoop-3.3.6 /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}

# =========================================================
# 3. HIVE 2.3.10
# =========================================================
ADD apache-hive-2.3.10-bin.tar.gz /opt/

RUN mv /opt/apache-hive-2.3.10-bin /opt/hive

ENV HIVE_HOME=/opt/hive
ENV PATH=${HIVE_HOME}/bin:${PATH}

# =========================================================
# 4. SPARK 3.5.3 - HADOOP 3 BUNDLE
# =========================================================
ADD spark-3.5.3-bin-hadoop3.tgz /opt/

RUN mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

# =========================================================
# 5. DELTA LAKE + S3A DEPENDENCIES
# =========================================================

# Delta Lake 3.3.2 - Spark 3.5.x - Scala 2.12
COPY docker/jars/delta-spark_2.12-3.3.2.jar \
     ${SPARK_HOME}/jars/

COPY docker/jars/delta-storage-3.3.2.jar \
     ${SPARK_HOME}/jars/

# Spark 3.5.3 bin-hadoop3 bundles Hadoop client 3.3.4.
# Therefore, S3A dependencies in Spark must also use 3.3.4.

# Remove any conflicting S3A libraries if present.
RUN rm -f ${SPARK_HOME}/jars/hadoop-aws-*.jar \
          ${SPARK_HOME}/jars/aws-java-sdk-bundle-*.jar

# Use matching Hadoop AWS and AWS SDK versions.
COPY docker/jars/hadoop-aws-3.3.4.jar \
     ${SPARK_HOME}/jars/

# The SDK is intentionally downloaded during the image build.  The exact
# filename is ignored by git and may differ between local checkouts.
RUN wget -q -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# =========================================================
# 6. HIVE LIBRARY COMPATIBILITY
# =========================================================

# Hive uses the separately installed Hadoop 3.3.6.
# Keep its S3A dependencies aligned with Hadoop 3.3.6.
RUN rm -f ${HIVE_HOME}/lib/guava-*.jar && \
    cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar \
       ${HIVE_HOME}/lib/ && \
    cp ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-*.jar \
       ${HIVE_HOME}/lib/ && \
    cp ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar \
       ${HIVE_HOME}/lib/

# =========================================================
# 7. CONFIGURATION FILES
# =========================================================

# Hadoop configuration
COPY docker/hadoop-conf/ ${HADOOP_CONF_DIR}/

RUN find ${HADOOP_CONF_DIR} -type f \
    -exec sed -i 's/\r$//' {} +

# Hive configuration
COPY docker/hadoop-conf/hive-site.xml ${HIVE_HOME}/conf/

# Spark configuration
COPY docker/hadoop-conf/hive-site.xml ${SPARK_HOME}/conf/
COPY docker/hadoop-conf/spark-defaults.conf ${SPARK_HOME}/conf/

RUN sed -i 's/\r$//' ${SPARK_HOME}/conf/spark-defaults.conf

# =========================================================
# 8. HADOOP USER CONFIGURATION
# =========================================================
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# =========================================================
# 9. STARTUP
# =========================================================
COPY docker/start.sh /start.sh

RUN sed -i 's/\r$//' /start.sh && \
    chmod +x /start.sh

WORKDIR /opt/hadoop

ENTRYPOINT ["/start.sh"]
