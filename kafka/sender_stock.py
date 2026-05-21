import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📈 Stock Sender is live! Pushing to stock_topic...")

stocks = ["AAPL", "GOOG", "TSLA", "MSFT"]
counter = 0

try:
    while True:
        # Pick a different stock symbol each second
        symbol = stocks[counter % len(stocks)]
        payload = {
            "ticker": symbol,
            "price": 150.0 + (counter * 1.5),
            "action": "BUY"
        }
        
        # Notice we are sending to a BRAND NEW topic name here!
        producer.send('stock_topic', value=payload)
        print(f"Sent Stock: {payload}")
        
        counter += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping stock sender...")
finally:
    producer.close()