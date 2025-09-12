import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

# -------------------------------------------------
# Spark session
# -------------------------------------------------
try:
    spark = SparkSession.builder.getOrCreate()
except ImportError:
    spark = None

# -------------------------------------------------
# Logger setup
# -------------------------------------------------
logger = logging.getLogger("TrialBalanceIngestion")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# -------------------------------------------------
# Parse Trial Balance Excel using metadata
# -------------------------------------------------
def parse_trial_balance_excel(metadata):
    excel_path = f"{metadata.file_path}/{metadata.file_name}"
    sheet_name = metadata.sheet_name

    try:
        logger.info(f"Reading Excel file: {excel_path} | Sheet: {sheet_name}")

        # Company names row
        company_names = pd.read_excel(
            excel_path, sheet_name=sheet_name,
            skiprows=metadata.company_names_row - 1, nrows=1,
            header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        # Date row
        date_values = pd.read_excel(
            excel_path, sheet_name=sheet_name,
            skiprows=metadata.date_row - 1, nrows=1,
            header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        # Metric row
        metric_values = pd.read_excel(
            excel_path, sheet_name=sheet_name,
            skiprows=metadata.metric_row - 1, nrows=1,
            header=None, engine="openpyxl"
        ).iloc[0, 2:].tolist()

        # Trial balance data
        df_data = pd.read_excel(
            excel_path, sheet_name=sheet_name,
            skiprows=metadata.data_start_row - 1, engine="openpyxl"
        )

        # Rename first two columns
        df_data.rename(
            columns={
                df_data.columns[0]: metadata.first_col_name,
                df_data.columns[1]: metadata.second_col_name
            },
            inplace=True
        )

        # Build structured rows
        structured_rows = []
        for _, row in df_data.iterrows():
            for i, company in enumerate(company_names):
                cost = row.iloc[i + 2]
                if pd.notna(cost):
                    structured_rows.append({
                        metadata.first_col_name: row[metadata.first_col_name],
                        metadata.second_col_name: row[metadata.second_col_name],
                        "Company_name": company,
                        "Date": date_values[i],
                        "Metric": metric_values[i],
                        "Cost": cost
                    })

        ledger_pdf = pd.DataFrame(structured_rows)
        logger.info(f"Parsed Excel to pandas DataFrame with {len(ledger_pdf)} rows.")

        return ledger_pdf

    except Exception as e:
        logger.error(f"Failed to parse Excel file {excel_path}: {e}", exc_info=True)
        raise


# -------------------------------------------------
# Convert pandas → Spark DataFrame
# -------------------------------------------------
def convert_to_spark_df(pdf):
    if spark is None:
        raise EnvironmentError("SparkSession is not available.")
    return spark.createDataFrame(pdf)


# -------------------------------------------------
# Read GL mapping CSV
# -------------------------------------------------
def read_gl_csv():
    gl_csv_path = "/Volumes/demo_catalog/finance_demo/raw/GL Validation.csv"
    try:
        logger.info(f"Reading GL Validation CSV from: {gl_csv_path}")
        df = spark.read.csv(gl_csv_path, header=True, inferSchema=True)
        logger.info(f"Loaded GL Validation CSV with {df.count()} records.")
        return df
    except Exception as e:
        logger.error(f"Failed to read GL CSV: {e}", exc_info=True)
        raise


# -------------------------------------------------
# Enrich ledger with GL mapping & write
# -------------------------------------------------
def enrich_and_write(ledger_sdf, gl_sdf, file_name):
    output_table = "demo_catalog.finance_demo.ledger_with_gl_mapping"

    try:
        logger.info("Filtering ledger accounts between 40000 and 100000")
        filtered_ledger_df = ledger_sdf.filter(
            (col("Account_number") >= 40000) & (col("Account_number") <= 100000)
        )

        # Deduplicate GL mapping
        window_spec = Window.partitionBy("Account_no").orderBy("GL_Category")
        gl_df_deduped = (
            gl_sdf.withColumn("row_num", row_number().over(window_spec))
                 .filter("row_num = 1")
                 .drop("row_num")
        )

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
        ).withColumn("file_name", lit(file_name))  # track source file

        logger.info(f"Writing enriched data to Delta table: {output_table}")
        joined_df.write.mode("append").saveAsTable(output_table)
        logger.info("Write operation successful.")

        return joined_df

    except Exception as e:
        logger.error(f"Failed during enrich and write step: {e}", exc_info=True)
        raise


# -------------------------------------------------
# Main driver
# -------------------------------------------------
def main():
    # Load all metadata rows
    metadata_rows = spark.sql("""
        SELECT * 
        FROM demo_catalog.finance_demo.trial_balance_metadata
    """).collect()

    if not metadata_rows:
        logger.warning("No metadata rows found. Nothing to process.")
        return

    logger.info(f"Found {len(metadata_rows)} metadata entries.")

    # Load GL mapping once
    gl_sdf = read_gl_csv()

    all_results = []

    for metadata in metadata_rows:
        try:
            logger.info(f"Processing file: {metadata.file_name}")

            # Parse Excel into pandas
            ledger_pdf = parse_trial_balance_excel(metadata)

            # 🔹 Save structured ledger to CSV (defined in metadata)
            output_csv = metadata.output_csv
            ledger_pdf.to_csv(output_csv, index=False)
            logger.info(f"Structured trial balance saved to CSV: {output_csv}")

            # Convert to Spark
            ledger_sdf = convert_to_spark_df(ledger_pdf)

            # Enrich & write
            result_df = enrich_and_write(ledger_sdf, gl_sdf, metadata.file_name)
            all_results.append(result_df)

            logger.info(f"✅ Finished processing {metadata.file_name}")

        except Exception as e:
            logger.error(f"❌ Failed to process {metadata.file_name}: {e}", exc_info=True)

    return all_results


# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    main()
