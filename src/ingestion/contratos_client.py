import json
import time
from pathlib import Path
from datetime import datetime
import requests

BASE_URL = "https://contratos.comprasnet.gov.br/api"  # :contentReference[oaicite:2]{index=2}

def _get_json(url: str, params: dict | None = None, retries: int = 3, backoff: float = 1.0) -> object:
    headers = {"accept": "application/json"}
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(backoff * (2 ** attempt))
    raise last_err

def fetch_contratos_por_ug(ug: str) -> list[dict]:
    url = f"{BASE_URL}/contrato/ug/{ug}"  # :contentReference[oaicite:3]{index=3}
    data = _get_json(url)
    # normalmente vem lista
    return data if isinstance(data, list) else data.get("items", [])

def cache_raw_json(payload: object, ug: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/raw/contratos")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"contratos_ug_{ug}_{ts}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path