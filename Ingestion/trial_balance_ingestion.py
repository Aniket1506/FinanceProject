import logging
import pandas as pd

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window
    spark = SparkSession.builder.getOrCreate()
except ImportError:
    spark = None

# Logger setup
logger = logging.getLogger("TrialBalanceIngestion")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def parse_trial_balance_excel():
    excel_path = "/Workspace/Repos/aniketson@cybage.com/FinanceProject/data/Trial Balance YTD Consolidating.xlsx"
    sheet_name = "Trial Balance YTD Consolidating"

    try:
        logger.info(f"Reading Excel file: {excel_path} | Sheet: {sheet_name}")

        company_names = pd.read_excel(
            excel_path, sheet_name=sheet_name, skiprows=7, nrows=1, header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        date_values = pd.read_excel(
            excel_path, sheet_name=sheet_name, skiprows=9, nrows=1, header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        metric_values = pd.read_excel(
            excel_path, sheet_name=sheet_name, skiprows=10, nrows=1, header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        df_data = pd.read_excel(excel_path, sheet_name=sheet_name, skiprows=11, engine="openpyxl")
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

        ledger_pdf = pd.DataFrame(structured_rows)
        logger.info(f"Parsed Excel to pandas DataFrame with {len(ledger_pdf)} rows.")

        return ledger_pdf

    except Exception as e:
        logger.error(f"Failed to parse Excel file: {e}", exc_info=True)
        raise


def convert_to_spark_df(pdf):
    if spark is None:
        raise EnvironmentError("SparkSession is not available.")
    return spark.createDataFrame(pdf)


def read_gl_csv():
    gl_csv_path = "/Volumes/demo_catalog/finance_demo/processed_files/GL Validation.csv"  # <-- Update this path accordingly

    try:
        logger.info(f"Reading GL Validation CSV from: {gl_csv_path}")
        df = spark.read.csv(gl_csv_path, header=True, inferSchema=True)
        logger.info(f"Loaded GL Validation CSV with {df.count()} records.")
        return df
    except Exception as e:
        logger.error(f"Failed to read GL CSV: {e}", exc_info=True)
        raise


def enrich_and_write(ledger_sdf, gl_sdf):
    output_table = "demo_catalog.finance_demo.ledger_with_gl_mapping"

    try:
        logger.info("Filtering ledger accounts between 40000 and 100000")
        filtered_ledger_df = ledger_sdf.filter(
            (col("Account_number") >= 40000) & (col("Account_number") <= 100000)
        )

        window_spec = Window.partitionBy("Account_no").orderBy("GL_Category")
        gl_df_deduped = gl_sdf.withColumn("row_num", row_number().over(window_spec)) \
                             .filter("row_num = 1") \
                             .drop("row_num")

        logger.info("Joining ledger with GL mapping...")
        joined_df = filtered_ledger_df.join(
            gl_df_deduped,
            filtered_ledger_df.Account_number == gl_df_deduped.Account_no,
            "left"
        ).select(
            filtered_ledger_df.Account_number,
            filtered_ledger_df.Company_name,
            gl_df_deduped.GL_Category,
            filtered_ledger_df.Cost,
            filtered_ledger_df.Date
        )

        logger.info(f"Writing enriched data to Delta table: {output_table}")
        joined_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
        logger.info("Write operation successful.")

        return joined_df

    except Exception as e:
        logger.error(f"Failed during enrich and write step: {e}", exc_info=True)
        raise


def main():
    ledger_pdf = parse_trial_balance_excel()

    if spark is None:
        logger.warning("Spark is not available; returning pandas DataFrame only.")
        return ledger_pdf

    ledger_sdf = convert_to_spark_df(ledger_pdf)
    gl_sdf = read_gl_csv()
    enrich_and_write(ledger_sdf, gl_sdf)


if __name__ == "__main__":
    main()
