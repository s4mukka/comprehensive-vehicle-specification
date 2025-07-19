FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    zip \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5
ENV SCALA_VERSION=2.12
ENV ICEBERG_VERSION=1.9.1
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=2.20.43

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

ENV SPARK_HOME /usr/local/lib/python3.12/site-packages/pyspark
RUN mkdir -p ${SPARK_HOME}/jars

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.788/aws-java-sdk-bundle-1.12.788.jar ${SPARK_HOME}/jars/

ENV SPARK_CONF_DIR=/opt/spark/conf
RUN mkdir -p ${SPARK_CONF_DIR}

ADD ./spark-defaults.conf ${SPARK_CONF_DIR}/spark-defaults.conf

COPY . .

CMD ["bash"]
