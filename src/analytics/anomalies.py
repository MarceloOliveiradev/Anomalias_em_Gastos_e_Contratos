import pandas as pd
import numpy as np

def robust_zscore(series: pd.Series) -> pd.Series:
    median = series.median()
    mad = (series.sub(median).abs()).median()
    if mad == 0 or pd.isna(mad):
        return pd.Series([0.0] * len(series), index=series.index)
    return 0.6745 * (series - median) / mad

def detect_anomalies(df: pd.DataFrame, group_cols: list[str], value_col: str, threshold: float = 3.5, min_points: int = 6) -> pd.DataFrame:
    df = df.copy()
    agg = df.groupby(group_cols, as_index=False)[value_col].sum()

    # Caso clássico: [fornecedor, mes] => base_group = fornecedor, time_col = mes
    if len(group_cols) >= 2:
        base_group = group_cols[:-1]
        time_col = group_cols[-1]

        # 1) tenta anomalia por histórico do fornecedor (se tiver pontos suficientes)
        counts = agg.groupby(base_group)[value_col].transform("count")
        z_hist = agg.groupby(base_group)[value_col].transform(robust_zscore)
        hist_flag = (z_hist.abs() > threshold) & (counts >= min_points)

        # 2) fallback: anomalia global dentro do "time_col" (ex: mês)
        z_global = agg.groupby(time_col)[value_col].transform(robust_zscore)
        global_flag = z_global.abs() > threshold

        agg["is_anomaly"] = hist_flag | global_flag
        return agg

    # Se só tem 1 grupo, faz global mesmo
    z = robust_zscore(agg[value_col])
    agg["is_anomaly"] = z.abs() > threshold
    return agg