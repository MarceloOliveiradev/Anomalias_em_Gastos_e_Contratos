# CÓDIGO RASCUNHO para uma futura atualização de FONTE de dados.

import pandas as pd
from src.ingestion.siconfi_client import SiconfiClient

# Observação: a API do SICONFI possui vários endpoints.
# Você vai escolher 1 endpoint de despesas/relatórios para seu MVP e depois expandir.
# A página oficial explica a API e regras de paginação. :contentReference[oaicite:3]{index=3}

def fetch_sample_dataset() -> pd.DataFrame:
    client = SiconfiClient("https://apidatalake.tesouro.gov.br/ords/siconfi")  # base comum em exemplos oficiais
    # Exemplo genérico: você ajusta "endpoint" e params conforme o conjunto escolhido.
    # Comece com um endpoint pequeno para provar o pipeline.

    endpoint = "pnd"  # placeholder: você vai substituir pelo endpoint real do conjunto escolhido
    rows = client.get(endpoint, params={"limit": 5000})
    return pd.DataFrame(rows)