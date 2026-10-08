FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# 1. Cài đặt các gói cơ bản, Java 11 và OPENSSH-SERVER
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    openssh-server \
    ssh \
    pdsh \
    vim \
    wget \
    curl \
    net-tools \
    iputils-ping \
    && apt-get clean

# --- CẤU HÌNH SSH ĐỂ PREFECT CÓ THỂ KẾT NỐI QUA NETWORK ---
RUN mkdir /var/run/sshd
RUN ssh-keygen -A
RUN echo 'root:1' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/session    required     pam_loginuid.so/session    optional     pam_loginuid.so/g' /etc/pam.d/sshd

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# 2. Cài đặt Hadoop 3.3.6
ADD hadoop-3.3.6.tar.gz /opt/
RUN mv /opt/hadoop-3.3.6 /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 3. Cài đặt Hive 2.3.10
ADD apache-hive-2.3.10-bin.tar.gz /opt/
RUN mv /opt/apache-hive-2.3.10-bin /opt/hive
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

# 4. Cài đặt Spark 3.5.3 (đổi từ 4.1.1)
ADD spark-3.5.3-bin-hadoop3.tgz /opt/
RUN mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 4b. Thêm Delta Lake (Cách A - copy jar đã tải sẵn từ docker/jars/)
COPY docker/jars/delta-spark_2.12-3.3.2.jar $SPARK_HOME/jars/
COPY docker/jars/delta-storage-3.3.2.jar $SPARK_HOME/jars/

# 5. FIX LỖI THƯ VIỆN & PHÂN QUYỀN SSH NỘI BỘ (HADOOP)
RUN rm -f $HIVE_HOME/lib/guava-*.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/ && \
    cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-*.jar $HIVE_HOME/lib/ && \
    cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar $HIVE_HOME/lib/

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile=/dev/null" >> /etc/ssh/ssh_config && \
    ssh-keygen -A

# 6. THIẾT LẬP CẤU HÌNH
COPY docker/config/* $HADOOP_CONF_DIR/
RUN find $HADOOP_CONF_DIR -type f -exec sed -i 's/\r$//' {} +
COPY docker/config/hive-site.xml $HIVE_HOME/conf/
COPY docker/config/hive-site.xml $SPARK_HOME/conf/
COPY docker/config/spark-defaults.conf $SPARK_HOME/conf/
RUN sed -i 's/\r$//' $SPARK_HOME/conf/spark-defaults.conf

# 7. BIẾN MÔI TRƯỜNG HADOOP USER
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# 8. KHỞI CHẠY
COPY docker/start.sh /start.sh
RUN sed -i 's/\r$//' /start.sh && chmod +x /start.sh
WORKDIR /opt/hadoop
ENTRYPOINT ["/start.sh"]