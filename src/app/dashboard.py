import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd

from src.analytics.anomalies import detect_anomalies
from src.ingestion.run_pipeline import run_pipeline

st.title("Radar de Anomalias em Gastos Públicos")

# =========================
# Pipeline automático
# =========================
st.subheader("Pipeline automático")

if st.button("Rodar pipeline agora"):
    info = run_pipeline()
    st.success(f"Fonte: {info.get('source')} | Linhas: {info.get('rows')} | CSV: {info.get('output_csv')}")
    st.json(info["quality_report"])

auto_csv_path = Path("data/processed/gastos_processados.csv")

use_auto = False
if auto_csv_path.exists():
    st.caption(f"CSV automático encontrado: {auto_csv_path}")
    use_auto = st.checkbox("Usar CSV automático (sem upload)", value=True)
else:
    st.warning("Nenhum CSV automático encontrado. Clique em 'Rodar pipeline agora' ou faça upload.")

# =========================
# Carregamento de dados
# =========================
df = None

if use_auto and auto_csv_path.exists():
    df = pd.read_csv(auto_csv_path)
else:
    uploaded = st.file_uploader("Envie um CSV processado (MVP)", type=["csv"])
    if uploaded:
        df = pd.read_csv(uploaded)

if df is None:
    st.info("Rode o pipeline ou envie um CSV para continuar.")
    st.stop()

# “prova de vida”
st.caption(f"Linhas carregadas: {len(df)} | Colunas: {len(df.columns)}")
st.write("Valores únicos (exemplo):")
st.write(
    {
        "mes (unique)": df["mes"].nunique() if "mes" in df.columns else None,
        "orgao (unique)": df["orgao"].nunique() if "orgao" in df.columns else None,
        "fornecedor (unique)": df["fornecedor"].nunique() if "fornecedor" in df.columns else None,
    }
)

# =========================
# Filtros (opcional)
# =========================
st.subheader("Filtros (opcional)")

col_filtro = st.selectbox("Filtrar por coluna", ["(nenhum)"] + list(df.columns))

if col_filtro != "(nenhum)":
    valores = sorted(df[col_filtro].dropna().astype(str).unique().tolist())
    escolhido = st.selectbox("Valor", ["(todos)"] + valores)

    if escolhido != "(todos)":
        df = df[df[col_filtro].astype(str) == escolhido].copy()

st.subheader("Amostra dos dados")
st.dataframe(df.sample(min(20, len(df))), use_container_width=True)

# =========================
# Configurar análise
# =========================
st.subheader("Configurar análise")

value_col = st.selectbox(
    "Coluna de valor",
    df.columns,
    index=list(df.columns).index("valor") if "valor" in df.columns else 0,
)

group_1 = st.selectbox(
    "Agrupar por (1)",
    df.columns,
    index=list(df.columns).index("fornecedor") if "fornecedor" in df.columns else 0,
)

group_2_options = ["(nenhum)"] + list(df.columns)
group_2 = st.selectbox(
    "Agrupar por (2) opcional",
    group_2_options,
    index=group_2_options.index("mes") if "mes" in df.columns else 0,
)

group_cols = [group_1] + ([] if group_2 == "(nenhum)" else [group_2])

# =========================
# Rodar detecção
# =========================
if st.button("Detectar anomalias"):
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0)

    res = detect_anomalies(df, group_cols=group_cols, value_col=value_col)
    res_sorted = res.sort_values(["is_anomaly", value_col], ascending=[False, False])

    st.subheader("Resultados (anomalias no topo)")
    st.dataframe(res_sorted.head(50), use_container_width=True)

    st.subheader("Somente anomalias")
    anom = res_sorted[res_sorted["is_anomaly"]]
    st.dataframe(anom, use_container_width=True)

    st.metric("Total de linhas analisadas", len(res_sorted))
    st.metric("Total de anomalias", int(anom.shape[0]))

    csv_bytes = res_sorted.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar resultados (CSV)",
        data=csv_bytes,
        file_name="resultados_anomalias.csv",
        mime="text/csv",
    )
else:
    st.info("Configure os filtros e clique em 'Detectar anomalias'.")