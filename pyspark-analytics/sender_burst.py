import json
import time
import random
from kafka import KafkaProducer

# 1. Initialize the Kafka Producer
print("Connecting to Kafka Broker...")
producer = KafkaProducer(
    # bootstrap_servers=['localhost:9092'],
    bootstrap_servers=['kafka-service:9092'],  # Use 'kafka' as the hostname in Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Blasting high-throughput data packets into 'weather_topic'...")

record_id = 1

try:
    while True:
        # Send 5,000 records all at once in a single burst!
        for _ in range(5000):
            data = {
                "id": record_id,
                "temperature": round(random.uniform(15.0, 40.0), 1),
                "status": "HEALTHY"
            }
            # Push to the local Kafka broker buffer
            producer.send('weather_topic', value=data)
            record_id += 1
        
        # Flush ensures all buffered messages are completely sent over the network
        producer.flush() 
        print(f"💥 Sent a burst of 5,000 records. (Total records generated: {record_id - 1})")
        
        # Wait 5 seconds before hitting it with another massive wave
        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopping data generation. Closing producer connection...")
    producer.close()