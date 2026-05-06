import json

from kafka import KafkaConsumer, KafkaProducer

from const import (
    KAFKA_BOOTSTRAP_SERVERS,
    PROCESSADOR_TAMANHO_BUFFER,
    TOPICO_TEMPERATURA,
    TOPICO_TEMPERATURA_PROCESSADA,
)

consumer = KafkaConsumer(
    TOPICO_TEMPERATURA,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

buffer = []

for msg in consumer:
    buffer.append(msg.value["temperatura"])

    if len(buffer) > PROCESSADOR_TAMANHO_BUFFER:
        buffer.pop(0)

    media = sum(buffer) / len(buffer)

    evento_processado = {
        "media": round(media, 2),
        "timestamp": msg.value["timestamp"]
    }

    producer.send(TOPICO_TEMPERATURA_PROCESSADA, evento_processado)

    print("Processado:", evento_processado)