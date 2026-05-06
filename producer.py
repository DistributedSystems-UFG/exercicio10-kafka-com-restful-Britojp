import json
import random
import time

from kafka import KafkaProducer

from const import (
    KAFKA_BOOTSTRAP_SERVERS,
    SENSOR_INTERVALO_SEGUNDOS,
    SENSOR_TEMPERATURA_MAX,
    SENSOR_TEMPERATURA_MIN,
    TOPICO_TEMPERATURA,
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    temp = round(random.uniform(SENSOR_TEMPERATURA_MIN, SENSOR_TEMPERATURA_MAX), 2)

    evento = {
        "temperatura": temp,
        "timestamp": time.time(),
    }

    producer.send(TOPICO_TEMPERATURA, evento)
    print("Enviado:", evento)

    time.sleep(SENSOR_INTERVALO_SEGUNDOS)