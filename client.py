import json
import time
import urllib.error
import urllib.request

from const import (
    API_BASE_URL,
    API_CAMINHO_MEDIA,
    API_CAMINHO_TEMPERATURAS,
    CLIENTE_INTERVALO_SEGUNDOS,
    CLIENTE_TIMEOUT_SEGUNDOS,
)


def obter_json(caminho_relativo):
    url = f"{API_BASE_URL.rstrip('/')}{caminho_relativo}"
    requisicao = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(requisicao, timeout=CLIENTE_TIMEOUT_SEGUNDOS) as resposta:
        return json.loads(resposta.read().decode("utf-8"))


def executar():
    while True:
        try:
            temperaturas = obter_json(API_CAMINHO_TEMPERATURAS)
            ultima_media = obter_json(API_CAMINHO_MEDIA)
            print(json.dumps(temperaturas, ensure_ascii=False, indent=2))
            print(json.dumps(ultima_media, ensure_ascii=False))
        except urllib.error.URLError as erro:
            print(erro)
        time.sleep(CLIENTE_INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    executar()
