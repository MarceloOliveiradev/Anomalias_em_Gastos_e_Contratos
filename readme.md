# Radar de Anomalias em Gastos Públicos (ComprasNet Contratos)

Projeto em Python que:

- Consome dados reais da API pública do ComprasNet (Contratos por UG)
- Gera um dataset processado (`CSV`) + relatório de qualidade (`JSON`)
- Detecta anomalias por agrupamento (mês/órgão/fornecedor)
- Exibe um dashboard interativo em Streamlit com filtros e export dos resultados

## Stack

- Python 3.11+
- pandas / numpy
- requests
- Streamlit

---

## Estrutura do projeto

radar-anomalias/
├─ data/

│ ├─ processed/ # arquivos gerados (CSV e relatório)

│ ├─ raw/

│ │ └─ contratos/ # JSON bruto (cache) da API ComprasNet

│ └─ samples/

│ └─ sample_gastos.csv # dataset offline para demo

├─ src/

│ ├─ app/ # Streamlit dashboard

│ ├─ ingestion/ # Coleta (ComprasNet) + pipeline

│ ├─ transform/ # Padronização + qualidade

│ └─ analytics/ # Detecção de anomalias

└─ tests/

---

## 1) Criar ambiente virtual (Windows / PowerShell)

Na raiz do projeto (`radar-anomalias/`):

```powershell
# (opcional) remover venv antigo
Remove-Item -Recurse -Force .\venv -ErrorAction SilentlyContinue

# criar e ativar
python -m venv venv
.\venv\Scripts\Activate

# atualizar pip
python -m pip install --upgrade pip

```

## 2) Instalar dependências

```
python -m pip install pandas numpy requests streamlit pydantic python-dotenv pytest
```

Dica: use sempre python -m pip (evita problemas com pip.exe no Windows).

## 3) Modos de execução

Você pode rodar o projeto de 2 formas:

A) Com dados reais (API ComprasNet)

```
$env:USE_REAL_DATA="1"
$env:UGS="113601,110161"
$env:MAX_CONTRATOS_POR_UG="300"
python -m streamlit run src/app/dashboard.py
```

- UGS: lista de UGs separadas por vírgula
- MAX_CONTRATOS_POR_UG: limita o volume por UG para controlar tempo/volume
- Cache bruto (raw) será salvo em: data/raw/contratos/\*.json

B) Offline (sample)

Usa o arquivo data/samples/sample_gastos.csv sem chamar API:

```
$env:USE_REAL_DATA="0"
python -m streamlit run src/app/dashboard.py
```

## 4) Rodar o pipeline manualmente (opcional)

Se você quiser gerar o CSV e o relatório sem abrir o dashboard:

```
python -c "from src.ingestion.run_pipeline import run_pipeline; print(run_pipeline())"
```

# Saídas geradas:

- data/processed/gastos_processados.csv
- data/processed/quality_report.json

## 5) Usando o dashboard

No dashboard:

Clique em Rodar pipeline agora (para atualizar os dados)

Use Filtros (opcional)

Configure o agrupamento (ex.: mes + fornecedor ou mes + orgao)

Clique em Detectar anomalias

Baixe o resultado em CSV no botão de download

## 6) Rodar testes

```
python -m pytest
```
