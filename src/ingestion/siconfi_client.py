# CÓDIGO RASCUNHO para uma futura atualização de FONTE de dados.

import requests

class SiconfiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        # algumas APIs retornam lista direto, outras retornam objeto com itens
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return [data]