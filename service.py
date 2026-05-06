import json
import threading

from fastapi import FastAPI
from kafka import KafkaConsumer

from const import KAFKA_BOOTSTRAP_SERVERS, TOPICO_TEMPERATURA_PROCESSADA

app = FastAPI()

database = []

def calcular_media(registros: list[dict]) -> float:
    if not registros:
        return 0.0
    valores = [float(r["media"]) for r in registros]
    return sum(valores) / len(valores)

def consumir_kafka():
    consumer = KafkaConsumer(
        TOPICO_TEMPERATURA_PROCESSADA,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for msg in consumer:
        data = msg.value

        database.append(data)

threading.Thread(target=consumir_kafka, daemon=True).start()

@app.get("/temperaturas")
def listar():
    return {"temperaturas": database}

@app.get("/temperaturas/media")
def ultima_media():
    trecho = [database[-1]] if database else []
    return {"temperatura": calcular_media(trecho)}

