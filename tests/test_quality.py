import pandas as pd
from src.transform.quality import quality_report

def test_quality_report_detects_missing_required_columns():
    df = pd.DataFrame({"a": [1], "b": [2]})
    report = quality_report(df, required=["mes", "valor"])
    assert "mes" in report["missing_required_columns"]
    assert "valor" in report["missing_required_columns"]

def test_quality_report_counts_rows_and_duplicates():
    df = pd.DataFrame({"x": [1, 1], "y": [2, 2]})
    report = quality_report(df, required=["x"])
    assert report["rows"] == 2
    assert report["duplicate_rate"] >= 0.5