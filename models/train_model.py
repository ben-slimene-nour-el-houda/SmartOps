from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os

def generate_synthetic_training_data(spark):
    """
    Generates a synthetic balanced dataset for demonstration.
    Labels are based on rule-logic with some added noise.
    """
    print("Generating synthetic training data...")
    # Generate 10k records
    raw_data = spark.range(0, 10000).select(
        (F.rand() * 10000).alias("amount"),
        (F.rand() * 24).cast("int").alias("tx_hour"),
        (F.rand() > 0.5).cast("int").alias("is_online"),
        F.element_at(F.array([F.lit("Tunis"), F.lit("Sousse"), F.lit("Sfax"), F.lit("FOREIGN_REMOTE")]), 
                     (F.rand() * 4 + 1).cast("int")).alias("location"),
        F.element_at(F.array([F.lit("GROCERY"), F.lit("ELECTRONICS"), F.lit("TRAVEL")]), 
                     (F.rand() * 3 + 1).cast("int")).alias("merchant_category")
    )

    # Core Logic for Labeling (Ground Truth)
    # High amount + Night or Foreign location = High fraud probability
    labeled_data = raw_data.withColumn("fraud_prob", (
        F.when(F.col("amount") > 5000, 0.4).otherwise(0.0) +
        F.when((F.col("tx_hour") < 6) | (F.col("tx_hour") > 22), 0.2).otherwise(0.0) +
        F.when(F.col("location") == "FOREIGN_REMOTE", 0.4).otherwise(0.0) +
        (F.rand() * 0.2) # Adding noise
    )).withColumn("label", F.when(F.col("fraud_prob") > 0.6, 1).otherwise(0))

    return labeled_data

def train_fraud_model():
    spark = SparkSession.builder.appName("SmartOps-ModelTraining").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load data
    processed_path = "./data/processed"
    if os.path.exists(processed_path) and len(os.listdir(processed_path)) > 0:
        print(f"Loading data from {processed_path}...")
        data = spark.read.parquet(processed_path)
        # Assuming we need a label, if not present, we use the logic above to create one for demo
        data = data.withColumn("label", F.when(F.col("risk_level") >= 7, 1).otherwise(0))
    else:
        data = generate_synthetic_training_data(spark)

    # Feature Engineering Pipeline
    categorical_cols = ["location", "merchant_category"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep") for col in categorical_cols]
    
    assembler = VectorAssembler(
        inputCols=["amount", "tx_hour", "is_online"] + [f"{col}_idx" for col in categorical_cols],
        outputCol="features"
    )

    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)

    pipeline = Pipeline(stages=indexers + [assembler, rf])

    # Split data
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    # Train
    print("Training Random Forest model...")
    model = pipeline.fit(train_data)

    # Evaluate
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    print(f"Model Training Complete. AUC: {auc:.4f}")

    # Save Model
    model_path = "./models/fraud_model"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    spark.stop()

if __name__ == "__main__":
    train_fraud_model()
