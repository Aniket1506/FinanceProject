import pandas as pd
from Ingestion.trial_balance_ingestion import parse_trial_balance

def test_parse_trial_balance():
    file_path = "data/Trial Balance YTD Consolidating.xlsx"  # place test file
    df = parse_trial_balance(file_path)

    assert isinstance(df, pd.DataFrame)
    assert "Account_number" in df.columns
    assert "Company_name" in df.columns
    assert not df.empty