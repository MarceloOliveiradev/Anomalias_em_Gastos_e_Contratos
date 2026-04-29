import pandas as pd
from datetime import datetime

from src.ingestion.contratos_client import fetch_contratos_por_ug, cache_raw_json


def _to_mes(date_str: str | None) -> str:
    """Converte datas tipo '2019-01-31' em '2019-01'."""
    if not date_str:
        return "desconhecido"
    try:
        # A API já traz ISO YYYY-MM-DD (ex: data_assinatura) :contentReference[oaicite:1]{index=1}
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return f"{dt.year}-{dt.month:02d}"
    except Exception:
        return "desconhecido"


def _parse_brl_number(x) -> float:
    """Converte '1.227.999,00' -> 1227999.0 e lida com None/0."""
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return 0.0
        # padrão BR: milhar com '.', decimal com ','
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0
    return 0.0


def _pick_first_value(contract: dict, keys: list[str]) -> float:
    """Pega o primeiro campo numérico existente (string BR) dentro do contrato."""
    for k in keys:
        if k in contract and contract.get(k) not in (None, "", "0,00", "0,0", "0"):
            return _parse_brl_number(contract.get(k))
    # se tudo falhar, tenta mesmo assim o primeiro
    for k in keys:
        if k in contract:
            return _parse_brl_number(contract.get(k))
    return 0.0


def _extract_orgao(contract: dict, ug_fallback: str) -> str:
    """
    Extrai um identificador de órgão/UG do bloco 'contratante'.
    Exemplo no JSON: contratante -> orgao -> unidade_gestora -> codigo/nome_resumido :contentReference[oaicite:2]{index=2}
    """
    try:
        ug = (
            contract.get("contratante", {})
            .get("orgao", {})
            .get("unidade_gestora", {})
        )
        codigo = ug.get("codigo")
        nome_resumido = ug.get("nome_resumido")
        if codigo and nome_resumido:
            return f"{codigo} - {nome_resumido}"
        if codigo:
            return str(codigo)
    except Exception:
        pass

    # fallback: unidade_compra ou a própria UG consultada
    return str(contract.get("unidade_compra") or ug_fallback)


def _extract_fornecedor(contract: dict) -> str:
    """
    fornecedor é um objeto: {'tipo':..., 'cnpj_cpf_idgener':..., 'nome':...} :contentReference[oaicite:3]{index=3}
    """
    f = contract.get("fornecedor")
    if isinstance(f, dict):
        nome = f.get("nome") or "desconhecido"
        doc = f.get("cnpj_cpf_idgener")
        return f"{nome} ({doc})" if doc else nome

    # fallback (se vier diferente)
    return str(f) if f else "desconhecido"


def contratos_to_df(ug: str, contratos: list[dict]) -> pd.DataFrame:
    rows = []

    for c in contratos:
        # mês: prioriza data_assinatura / data_publicacao / vigencia_inicio :contentReference[oaicite:4]{index=4}
        data_ref = c.get("data_assinatura") or c.get("data_publicacao") or c.get("vigencia_inicio")
        mes = _to_mes(data_ref)

        orgao = _extract_orgao(c, ug_fallback=ug)
        fornecedor = _extract_fornecedor(c)

        # valor: prioriza global/inicial/acumulado/parcela :contentReference[oaicite:5]{index=5}
        valor = _pick_first_value(
            c,
            keys=["valor_global", "valor_inicial", "valor_acumulado", "valor_parcela"],
        )

        rows.append(
            {
                "mes": mes,
                "orgao": orgao,
                "fornecedor": fornecedor,
                "valor": valor,
            }
        )

    return pd.DataFrame(rows)


def fetch_contratos_dataset(ugs: list[str], max_contratos_por_ug: int = 500) -> pd.DataFrame:
    frames = []

    for ug in ugs:
        contratos = fetch_contratos_por_ug(ug)

        # cache do raw (B1)
        cache_raw_json(contratos, ug=ug)

        # limite (B2)
        if max_contratos_por_ug:
            contratos = contratos[:max_contratos_por_ug]

        frames.append(contratos_to_df(ug, contratos))

    if not frames:
        return pd.DataFrame(columns=["mes", "orgao", "fornecedor", "valor"])

    return pd.concat(frames, ignore_index=True)