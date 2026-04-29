from pathlib import Path
import os
import pandas as pd

from src.transform.clean import standardize_columns
from src.transform.quality import quality_report
from src.ingestion.fetch_contratos import fetch_contratos_dataset

OUTPUT_CSV = Path("data/processed/gastos_processados.csv")
QUALITY_JSON = Path("data/processed/quality_report.json")


def run_pipeline() -> dict:
    use_real = os.getenv("USE_REAL_DATA", "1") == "1"

    if use_real:
        ugs = os.getenv("UGS", "113601,110161").split(",")
        ugs = [u.strip() for u in ugs if u.strip()]
        max_por_ug = int(os.getenv("MAX_CONTRATOS_POR_UG", "300"))

        df = fetch_contratos_dataset(ugs=ugs, max_contratos_por_ug=max_por_ug)
        source = "comprasnet-contratos"
    else:
        # fallback (sem API): usa dataset de demonstração
        sample_inputs = [
            Path("data/samples/sample_gastos.csv"),
        ]
        sample_path = next((p for p in sample_inputs if p.exists()), None)
        if sample_path is None:
            raise FileNotFoundError("Não encontrei sample_gastos.csv em data/samples.")
        df = pd.read_csv(sample_path)
        source = "sample_csv"

    df = standardize_columns(df)

    required = ["mes", "orgao", "fornecedor", "valor"]
    report = quality_report(df, required=required)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    QUALITY_JSON.write_text(
        pd.Series(report).to_json(force_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "source": source,
        "rows": int(len(df)),
        "output_csv": str(OUTPUT_CSV),
        "quality_report": report,
    }


if __name__ == "__main__":
    print(run_pipeline())