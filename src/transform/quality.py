import pandas as pd

def quality_report(df: pd.DataFrame, required: list[str]) -> dict:
    missing_cols = [c for c in required if c not in df.columns]
    null_rates = {c: float(df[c].isna().mean()) for c in df.columns}
    dup_rate = float(df.duplicated().mean())

    return {
        "rows": int(len(df)),
        "missing_required_columns": missing_cols,
        "duplicate_rate": dup_rate,
        "null_rates": null_rates,
    }