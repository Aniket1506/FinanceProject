import os
import pandas as pd
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except ImportError:
    spark = None


def parse_trial_balance(file_path: str, sheet_name: str = "Trial Balance YTD Consolidating") -> pd.DataFrame:
    """
    Parse Trial Balance Excel into a structured pandas DataFrame.
    """

    # Read company names (row 8)
    company_names = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=7, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    # Read dates (row 10)
    date_values = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=9, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    # Read metrics (row 11)
    metric_values = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=10, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    # Read actual data (starting from row 12)
    df_data = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=11, engine="openpyxl")

    # Rename first two columns
    df_data.rename(columns={df_data.columns[0]: "Account_number", df_data.columns[1]: "Ledger"}, inplace=True)

    # Build structured dataset
    structured_rows = []
    for _, row in df_data.iterrows():
        for i, company in enumerate(company_names):
            cost = row.iloc[i + 2]  # values start from 3rd column
            if pd.notna(cost):
                structured_rows.append({
                    "Account_number": row["Account_number"],
                    "Ledger": row["Ledger"],
                    "Company_name": company,
                    "Date": date_values[i],
                    "Metric": metric_values[i],
                    "Cost": cost
                })

    return pd.DataFrame(structured_rows)


def write_to_delta(df: pd.DataFrame, table_name: str, catalog: str = "demo_catalog", schema: str = "finance_demo"):
    """
    Convert pandas DF → Spark DF → Delta table (Databricks only).
    """
    if spark is None:
        print("⚠️ Spark not available. Skipping Delta write. Running locally only.")
        return

    # spark_df = spark.createDataFrame(df)

    full_table_name = f"{catalog}.{schema}.{table_name}"
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print(f"✅ Written to Delta table: {full_table_name}")


if __name__ == "__main__":
    # Local dev mode
    file_path = Path("data/Trial Balance YTD Consolidating.xlsx")  # <-- put Excel under /data locally
    df = parse_trial_balance(file_path)
    print(df.head())

    # Save locally for validation
    df.to_csv("structured_trial_balance.csv", index=False)
    print("✅ Structured trial balance saved locally.")

    # In Databricks, also write to Delta
    write_to_delta(df, table_name="raw_ledger_data")
