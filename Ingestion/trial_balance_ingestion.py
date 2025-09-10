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
    company_names = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=7, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    date_values = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=9, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    metric_values = pd.read_excel(
        file_path, sheet_name=sheet_name, skiprows=10, nrows=1, header=None, engine="openpyxl"
    ).iloc[0, 2:].tolist()

    df_data = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=11, engine="openpyxl")

    df_data.rename(columns={df_data.columns[0]: "Account_number", df_data.columns[1]: "Ledger"}, inplace=True)

    structured_rows = []
    for _, row in df_data.iterrows():
        for i, company in enumerate(company_names):
            cost = row.iloc[i + 2]
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

    spark_df = spark.createDataFrame(df)

    full_table_name = f"{catalog}.{schema}.{table_name}"
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print(f"✅ Written to Delta table: {full_table_name}")


def enrich_with_gl_mapping(ledger_table: str, gl_csv_path: str, output_table: str):
    """
    Joins raw ledger data with GL mapping from CSV and writes to a new table.
    """

    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    ledger_df = spark.table(ledger_table)

    gl_df = spark.read.csv(gl_csv_path, header=True, inferSchema=True)

    # Filter for account numbers in the range
    filtered_ledger_df = ledger_df.filter(
        (col("Account_number") >= 40000) & (col("Account_number") <= 100000)
    )

    # Deduplicate GL validation records
    window_spec = Window.partitionBy("Account_no").orderBy("Gl_category")
    gl_df_deduped = gl_df.withColumn("row_num", row_number().over(window_spec)) \
                         .filter("row_num = 1") \
                         .drop("row_num")

    # Join and select desired columns
    joined_df = filtered_ledger_df.join(
        gl_df_deduped,
        filtered_ledger_df.Account_number == gl_df_deduped.Account_no,
        "left"
    ).select(
        "Account_number",
        "Company_name",
        "Gl_category",
        "Cost",
        "Date"
    )

    # Write output to Delta
    joined_df.write.mode("overwrite").saveAsTable(output_table)
    print(f"✅ GL Mapping Table written: {output_table}")


if __name__ == "__main__":
    # Load Excel file
    file_path = Path("/Workspace/Repos/aniketson@cybage.com/FinanceProject/data/Trial Balance YTD Consolidating.xlsx")
    df = parse_trial_balance(file_path)
    print(df.head())

    # Write raw ledger data
    write_to_delta(df, table_name="raw_ledger_data", catalog="finance_demo", schema="raw")

    # Enrich with GL mapping and write to new table
    gl_csv_path = "/Workspace/Repos/aniketson@cybage.com/FinanceProject/data/GL Validation.csv"
    enrich_with_gl_mapping(
        ledger_table="finance_demo.raw.raw_ledger_data",
        gl_csv_path=gl_csv_path,
        output_table="finance_demo.raw.ledger_with_gl_mapping"
    )
