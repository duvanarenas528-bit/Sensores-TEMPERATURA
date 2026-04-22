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
        "sensor_id": f"pres_{random.randint(1,3)}",
        "tipo": "presion",
        "valor": round(random.uniform(900, 1100), 2),  # presión en hPa
        "timestamp": int(time.time())
    }

    producer.send(TOPIC, value=data)
    print("Presión enviada:", data)

    time.sleep(2)