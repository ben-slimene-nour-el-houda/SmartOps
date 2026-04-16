#!/bin/bash

# Configuration
SPARK_VERSION="4.1.1"
KAFKA_VERSION="0-10_2.13"
PACKAGE_NAME="org.apache.spark:spark-sql-kafka-${KAFKA_VERSION}:${SPARK_VERSION}"

echo "Starting Spark Job: SmartOps Ingestion Service..."
echo "Using Package: ${PACKAGE_NAME}"

# Run the spark job
# Ensure .env is loaded or variables are set in Shell
export $(grep -v '^#' .env | xargs)

spark-submit \
    --packages ${PACKAGE_NAME} \
    services/ingestion-service/ingestion_service.py
