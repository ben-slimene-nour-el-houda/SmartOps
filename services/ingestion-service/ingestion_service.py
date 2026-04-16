import logging
import os
import json
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, when, hour, lit, current_timestamp, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, BooleanType, TimestampType, MapType
)
from pyspark.ml import PipelineModel

# Load configuration
load_dotenv()

# Logger Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingestion_service.log")
    ]
)
logger = logging.getLogger("SmartOps.Ingestion")

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TRANSACTION_TOPIC", "transactions")
SCORED_TOPIC = os.getenv("SCORED_TOPIC", "scored-transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./data/checkpoints")
PROCESSED_PATH = os.getenv("PROCESSED_DATA_PATH", "./data/processed")
FRAUD_PATH = os.getenv("FRAUD_CASES_PATH", "./data/fraud_cases")
MODEL_PATH = os.getenv("MODEL_PATH", "./models/fraud_model")
ML_WEIGHT = float(os.getenv("ML_WEIGHT", 0.6))

# Define Schema
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("location", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("is_online", BooleanType(), True),
    StructField("device_id", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("SmartOps-FraudIntelligence") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .getOrCreate()

def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    logger.info(f"Processing Batch {batch_id} - {batch_df.count()} records")

    # 1. Persistence (Parquet)
    batch_df.write.format("parquet").mode("append").save(PROCESSED_PATH)

    # 2. Scored Transactions to Kafka
    batch_df.selectExpr("user_id AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", SCORED_TOPIC) \
        .save()

    # 3. Fraud Alerts to Kafka
    alerts_df = batch_df.filter(col("prediction") == "FRAUD")
    if not alerts_df.isEmpty():
        logger.warning(f"Batch {batch_id}: {alerts_df.count()} FRAUD ALERTS!")
        alerts_df.selectExpr("transaction_id AS key", "to_json(struct(transaction_id, user_id, risk_level, fraud_probability, reason)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", ALERTS_TOPIC) \
            .save()
        
        # Also persist to fraud_cases for Phase 2 compatibility
        alerts_df.write.format("json").mode("append").save(FRAUD_PATH)

    # 4. Console Summary
    print("\n" + "="*80)
    print(f" 🧠 SMART OPS INTELLIGENCE | BATCH: {batch_id} ")
    print("="*80)
    batch_df.select(
        "transaction_id", "user_id", "amount", "fraud_probability", "final_fraud_score", "prediction"
    ).show(5, truncate=False)

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Load ML Model
    try:
        logger.info(f"Loading Intelligent Model from {MODEL_PATH}...")
        model = PipelineModel.load(MODEL_PATH)
        logger.info("Model loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load model: {e}. Ensure you ran models/train_model.py first.")
        return

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parsing
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("is_online", col("is_online").cast("int")) # Model expects int

    # Feature Engineering (Features needed for ML)
    enriched_df = parsed_df \
        .withColumn("tx_timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("tx_hour", hour(col("tx_timestamp")))

    # Real-Time ML Inference
    predictions_df = model.transform(enriched_df)
    
    # Extract Fraud Probability (from probability vector)
    # probability[1] is the fraud class
    get_fraud_prob = udf(lambda v: float(v[1]), DoubleType())
    
    inference_df = predictions_df.withColumn("fraud_probability", get_fraud_prob(col("probability")))

    # Decision Engine (Phase 3 Hybrid Logic)
    # Rule Score (Simplified rule for hybrid)
    inference_df = inference_df.withColumn("rule_score", (
        when(col("amount") > 5000, 0.5).otherwise(0.0) +
        when((col("tx_hour") < 5), 0.3).otherwise(0.0) +
        when(col("location") == "FOREIGN_REMOTE", 0.2).otherwise(0.0)
    ).cast("double"))

    # Final Combined Score
    # final_score = (ML Probability * Weight) + (Rule Score * (1 - Weight))
    final_df = inference_df.withColumn("final_fraud_score", 
        (col("fraud_probability") * lit(ML_WEIGHT)) + (col("rule_score") * lit(1.0 - ML_WEIGHT))
    ).withColumn("risk_level", (col("final_fraud_score") * 10).cast("int")) \
    .withColumn("prediction", 
        when(col("final_fraud_score") > 0.7, "FRAUD")
        .when(col("final_fraud_score") > 0.4, "SUSPICIOUS")
        .otherwise("NORMAL")
    ).withColumn("reason",
        when(col("final_fraud_score") > 0.7, "High ML Alert + Risky Rules")
        .when(col("fraud_probability") > 0.8, "Extreme ML Anomaly")
        .when(col("rule_score") > 0.8, "High Rule Trigger")
        .otherwise("Normal activity")
    )

    # Sink
    query = final_df.writeStream \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()