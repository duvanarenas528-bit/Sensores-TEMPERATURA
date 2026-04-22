from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "sensores"

while True:
    data = {
        "sensor_id": f"temp_{random.randint(1,3)}",
        "tipo": "temperatura",
        "valor": round(random.uniform(15, 100), 2),  # temperatura realista
        "timestamp": int(time.time())
    }

    producer.send(TOPIC, value=data)
    print("Temp enviado:", data)

    time.sleep(1)