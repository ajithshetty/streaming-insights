FROM eclipse-temurin:17-jdk-jammy

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 python3-pip python3-venv python3-dev build-essential wget && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# SQL connector jars for Kafka + JDBC/Postgres, used by job.py's Table API
RUN mkdir -p /opt/flink-connectors && \
    wget -q -P /opt/flink-connectors \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY job.py .

CMD ["python3", "job.py"]