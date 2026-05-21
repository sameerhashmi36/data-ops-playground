import time
import json
from kafka import KafkaProducer

# 1. Connect to the Kafka Broker running via Docker on port 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # Automatically convert Python dictionaries/JSON to bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Sender is live! Pushing stream to Kafka...")

counter = 1
try:
    while True:
        # Create a mock metrics payload
        payload = {
            "id": counter,
            "temperature": 25.0 + (counter % 5),
            "status": "HEALTHY"
        }
        
        # Send data to the channel named 'weather_topic'
        producer.send('weather_topic', value=payload)
        print(f"Sent: {payload}")
        
        counter += 1
        time.sleep(1)  # Wait 1 second before streaming the next data point

except KeyboardInterrupt:
    print("\nStopping sender...")
finally:
    producer.close()