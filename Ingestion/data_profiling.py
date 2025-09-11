import pandas as pd
from pathlib import Path

from trial_balance_ingestion import parse_trial_balance

def profile_trial_balance(df: pd.DataFrame, consolidated_company: str = "All Locations") -> dict:
    """
    Perform data profiling on structured trial balance DataFrame.
    Returns a profiling summary dictionary.
    """
    profile_summary = {}

    # --- Column Profiling ---
    profile_summary["missing_values"] = df.isna().sum().to_dict()
    profile_summary["missing_percent"] = (df.isna().mean() * 100).to_dict()

    profile_summary["duplicate_records"] = df.duplicated().sum()

    # Account number stats
    profile_summary["account_number_stats"] = {
        "min": df["Account_number"].min(),
        "max": df["Account_number"].max(),
        "unique_count": df["Account_number"].nunique()
    }

    # Cost distribution
    profile_summary["cost_stats"] = {
        "min": df["Cost"].min(),
        "max": df["Cost"].max(),
        "mean": df["Cost"].mean(),
        "std_dev": df["Cost"].std()
    }

    # --- Cross-column Profiling ---
    profile_summary["negative_costs"] = df[df["Cost"] < 0].shape[0]

    # Check for invalid dates
    if pd.api.types.is_datetime64_any_dtype(df["Date"]):
        profile_summary["date_range"] = {
            "min_date": df["Date"].min(),
            "max_date": df["Date"].max()
        }
    else:
        profile_summary["date_range"] = "⚠️ Date column is not parsed as datetime."

    # --- Cross-table Profiling ---
    if consolidated_company in df["Company_name"].unique():
        # Consolidated vs. sum of subsidiaries
        consolidated = df[df["Company_name"] == consolidated_company] \
            .groupby("Account_number")["Cost"].sum()

        subsidiaries = df[df["Company_name"] != consolidated_company] \
            .groupby("Account_number")["Cost"].sum()

        mismatches = (consolidated - subsidiaries).abs()
        mismatches = mismatches[mismatches > 1e-6]  # tolerance
        profile_summary["consolidation_mismatches"] = mismatches.to_dict()
    else:
        profile_summary["consolidation_mismatches"] = "⚠️ No consolidated company column found."

    # --- Data Rule Validation ---
    # Trial Balance Rule: Debits = Credits (simplified assumption: sum should be 0)
    total_cost = df.groupby("Company_name")["Cost"].sum().to_dict()
    profile_summary["debits_vs_credits"] = total_cost

    return profile_summary


# Example usage after parsing trial balance:
if __name__ == "__main__":
    file_path = Path("data/Trial Balance YTD Consolidating.xlsx")
    df = parse_trial_balance(file_path)

    profiling_report = profile_trial_balance(df)
    for section, details in profiling_report.items():
        print(f"\n--- {section.upper()} ---")
        print(details)
