from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

cities = [
    "Delhi",
    "Mumbai",
    "Bangalore",
    "Gurgaon",
    "Hyderabad"
]

payment_methods = [
    "UPI",
    "CARD",
    "COD",
    "NETBANKING"
]

for i in range(1, 101):

    order = {
        "order_id": f"ORD{10000 + i}",
        "customer_id": f"C{random.randint(1001, 1100)}",
        "product_id": f"P{random.randint(100, 120)}",
        "amount": random.randint(199, 150000),
        "city": random.choice(cities),
        "payment_method": random.choice(payment_methods),
        "timestamp": datetime.now().isoformat()
    }

    # Send order to the NEW Kafka topic
    producer.send("ecommerce_orders", value=order)
    producer.flush()

    print("Sent:", order)

    time.sleep(2)

producer.close()

print("\nAll 100 orders sent successfully!")