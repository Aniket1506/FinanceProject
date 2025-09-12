import pandas as pd
import logging
from pathlib import Path

# Import your parsing function (make sure it accepts a file path now)
from trial_balance_ingestion import parse_trial_balance_excel

logger = logging.getLogger("TrialBalanceProfiling")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def profile_trial_balance(df: pd.DataFrame, consolidated_company: str = "All Locations") -> dict:
    """
    Perform data profiling on structured trial balance DataFrame.
    Returns a profiling summary dictionary.
    """
    profile_summary = {}

    logger.info("Starting data profiling...")

    # --- Column Profiling ---
    profile_summary["missing_values"] = df.isna().sum().to_dict()
    profile_summary["missing_percent"] = (df.isna().mean() * 100).round(2).to_dict()

    profile_summary["duplicate_records"] = int(df.duplicated().sum())

    # Account number stats
    profile_summary["account_number_stats"] = {
        "min": int(df["Account_number"].min()),
        "max": int(df["Account_number"].max()),
        "unique_count": int(df["Account_number"].nunique())
    }

    # Cost distribution
    profile_summary["cost_stats"] = {
        "min": float(df["Cost"].min()),
        "max": float(df["Cost"].max()),
        "mean": float(df["Cost"].mean()),
        "std_dev": float(df["Cost"].std())
    }

    # --- Cross-column Profiling ---
    negative_costs_count = df[df["Cost"] < 0].shape[0]
    profile_summary["negative_costs_count"] = int(negative_costs_count)

    # Date column profiling
    if pd.api.types.is_datetime64_any_dtype(df["Date"]):
        profile_summary["date_range"] = {
            "min_date": str(df["Date"].min().date()),
            "max_date": str(df["Date"].max().date())
        }
    else:
        profile_summary["date_range"] = "⚠️ Date column is not parsed as datetime."

    # --- Cross-table Profiling ---
    if consolidated_company in df["Company_name"].unique():
        consolidated = df[df["Company_name"] == consolidated_company].groupby("Account_number")["Cost"].sum()
        subsidiaries = df[df["Company_name"] != consolidated_company].groupby("Account_number")["Cost"].sum()
        mismatches = (consolidated - subsidiaries).abs()
        mismatches = mismatches[mismatches > 1e-6]  # tolerance threshold
        profile_summary["consolidation_mismatches"] = mismatches.to_dict()
    else:
        profile_summary["consolidation_mismatches"] = "⚠️ No consolidated company column found."

    # --- Data Rule Validation ---
    total_cost_by_company = df.groupby("Company_name")["Cost"].sum().to_dict()
    profile_summary["debits_vs_credits"] = total_cost_by_company

    logger.info("Data profiling completed.")
    return profile_summary


if __name__ == "__main__":
    # Provide your Excel file path here (consistent with your ingestion code)
    file_path = Path("/Workspace/Repos/aniketson@cybage.com/FinanceProject/data/Trial Balance YTD Consolidating.xlsx")

    try:
        # parse_trial_balance_excel now expects excel_path argument
        df = parse_trial_balance_excel()

        report = profile_trial_balance(df)

        for section, content in report.items():
            print(f"\n--- {section.upper()} ---")
            print(content)

    except Exception as e:
        logger.error(f"Failed to profile trial balance data: {e}", exc_info=True)
