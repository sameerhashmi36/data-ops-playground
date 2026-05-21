import json
from kafka import KafkaConsumer

# 1. Setup consumer to connect to the broker and look for 'weather_topic'
consumer = KafkaConsumer(
    'weather_topic', 'stock_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Start reading from the very first message if missing a checkpoint
    # Automatically convert received raw bytes back into a Python dictionary
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📥 Receiver is active! Listening for incoming stream data...")

try:
    # This loop stays open, pulling new messages from Kafka automatically
    for message in consumer:
        data = message.value
        print(data)

except KeyboardInterrupt:
    print("\nStopping receiver...")
finally:
    consumer.close()