import json
import logging
import os
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Load configuration
load_dotenv()

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [ALERT-SYSTEM] - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/alerts.log")
    ]
)
logger = logging.getLogger("SmartOps.Alerts")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'alert-service-group',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([ALERTS_TOPIC])

    logger.info(f"Alert System started. Listening to {ALERTS_TOPIC}...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            # Process Alert
            try:
                alert_data = json.loads(msg.value().decode('utf-8'))
                
                # Visual Alert Formatting
                print("\n" + "!"*50)
                print(f"🚨 FRAUD ALERT DETECTED 🚨")
                print(f"Transaction ID: {alert_data.get('transaction_id')}")
                print(f"User ID:        {alert_data.get('user_id')}")
                print(f"Risk Level:     {alert_data.get('risk_level')}/10")
                print(f"Probability:    {alert_data.get('fraud_probability'):.2f}")
                print(f"Reason:         {alert_data.get('reason')}")
                print("!"*50 + "\n")
                
                logger.warning(f"Fraud alert for user {alert_data.get('user_id')} - Score: {alert_data.get('risk_level')}")
                
            except Exception as e:
                logger.error(f"Error parsing alert: {e}")

    except KeyboardInterrupt:
        logger.info("Alert System stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
