FROM python:3.12-slim

# Install OpenJDK 17 (recommended for Spark 4.1)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Kafka connector jars for Spark (optional if using --packages in spark-submit, but good for offline stability)
# RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar

COPY . .

CMD ["python", "ingestion_service.py"]
