import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TRANSACTION_TOPIC", "transactions")
PRODUCER_INTERVAL = float(os.getenv("PRODUCER_INTERVAL", 1.0))
VELOCITY_PROBABILITY = float(os.getenv("PRODUCER_VELOCITY_PROBABILITY", 0.1))
TUNISIAN_CITIES = os.getenv("LOCATIONS", "Tunis,Sousse,Sfax,Monastir").split(",")

fake = Faker()

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Reduced logging for cleaner console in Phase 2
        pass

def generate_transaction(user_id=None, location=None, device_id=None):
    """Generates a realistic transaction with optional fixed parameters for velocity attacks."""
    user_id = user_id or f"USER_{random.randint(1000, 9999)}"
    device_id = device_id or f"DEV_{random.randint(100, 999)}"
    
    # Regular transaction logic
    amount = round(random.uniform(5.0, 500.0), 3)  # TND
    location = location or random.choice(TUNISIAN_CITIES)
    payment_method = random.choice(["CARD", "MOBILE_PAYMENT", "BANK_TRANSFER", "CASH"])
    merchant_category = random.choice(["GROCERY", "ELECTRONICS", "RESTAURANT", "TRAVEL", "FASHION", "PHARMACY"])
    
    # Occasional Fraud Hint (High amount or risky category)
    is_suspicious = random.random() < 0.05  # 5% chance of being suspicious
    if is_suspicious:
        amount = round(random.uniform(2000.0, 10000.0), 3)
        location = "FOREIGN_REMOTE" if random.random() < 0.5 else location
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now().isoformat(),
        "amount": amount,
        "currency": "TND",
        "location": location,
        "payment_method": payment_method,
        "merchant_category": merchant_category,
        "is_online": random.choice([True, False]),
        "device_id": device_id
    }

def main():
    # Kafka Producer instance
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'transaction-producer'
    }
    
    producer = Producer(conf)
    
    print(f"Starting SmartOps Transaction Producer (v2.0)...")
    print(f"Targeting Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {TOPIC}")
    print(f"Interval: {PRODUCER_INTERVAL}s | Velocity Attack Prob: {VELOCITY_PROBABILITY}")
    
    try:
        while True:
            # Determine if we trigger a velocity attack (burst for same user)
            is_velocity_attack = random.random() < VELOCITY_PROBABILITY
            
            if is_velocity_attack:
                attack_user = f"VOL_{random.randint(1000, 9999)}"
                attack_device = f"DEV_BOT_{random.randint(10, 99)}"
                attack_location = random.choice(TUNISIAN_CITIES)
                burst_count = random.randint(5, 12)
                
                print(f"!!! TRIGGERING VELOCITY ATTACK: User {attack_user} | {burst_count} transactions !!!")
                
                for _ in range(burst_count):
                    tx = generate_transaction(user_id=attack_user, location=attack_location, device_id=attack_device)
                    producer.produce(TOPIC, key=tx['user_id'], value=json.dumps(tx).encode('utf-8'), callback=delivery_report)
                    producer.poll(0)
                    time.sleep(0.1)  # High frequency
                
                print(f"Velocity attack finished.")
            else:
                # Normal transaction
                tx = generate_transaction()
                print(f"Sending Transaction: {tx['transaction_id']} | {tx['amount']} {tx['currency']} from {tx['location']}")
                
                producer.produce(
                    TOPIC, 
                    key=tx['user_id'], 
                    value=json.dumps(tx).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                time.sleep(PRODUCER_INTERVAL)
            
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()