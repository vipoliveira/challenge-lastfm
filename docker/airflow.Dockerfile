FROM apache/airflow:2.9.1-python3.11

USER root

# Install Java for Spark
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Environment for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

# Install Spark
RUN mkdir -p ${SPARK_HOME} \
    && curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 \
    && rm /tmp/spark.tgz

# Python dependencies
COPY docker/requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow