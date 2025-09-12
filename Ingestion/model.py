# Databricks production script to build Trial Balance Data Model
# Author: Your Name

import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------
# CONFIGURATION
# -------------------------
CATALOG = "demo_catalog"
SCHEMA = "finance_demo"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.ledger_with_gl_mapping"

DIM_ACCOUNT = f"{CATALOG}.{SCHEMA}.dim_account"
DIM_COMPANY = f"{CATALOG}.{SCHEMA}.dim_company"
DIM_DATE = f"{CATALOG}.{SCHEMA}.dim_date"
FACT_LEDGER = f"{CATALOG}.{SCHEMA}.fact_ledger"

# -------------------------
# LOGGING CONFIG
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

try:
    logger.info("🚀 Starting data model build job")

    # -------------------------
    # LOAD SOURCE
    # -------------------------
    ledger_df = spark.table(SOURCE_TABLE)
    logger.info(f"Loaded source table {SOURCE_TABLE} with {ledger_df.count()} rows")

    # -------------------------
    # DIM ACCOUNT
    # -------------------------
    dim_account = (
        ledger_df
        .select("Account_no", "GL_Name", "GL_Category")
        .distinct()
        .withColumn("account_id", 
                    F.row_number().over(Window.orderBy("Account_no")))
    )
    dim_account.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_ACCOUNT)
    logger.info(f"Created {DIM_ACCOUNT} with {dim_account.count()} rows")

    # -------------------------
    # DIM COMPANY
    # -------------------------
    dim_company = (
        ledger_df
        .select("Company_name")
        .distinct()
        .withColumn("company_id", 
                    F.row_number().over(Window.orderBy("Company_name")))
    )
    dim_company.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_COMPANY)
    logger.info(f"Created {DIM_COMPANY} with {dim_company.count()} rows")

    # -------------------------
    # DIM DATE
    # -------------------------
    dim_date = (
        ledger_df
        .select(F.col("Date").alias("full_date"))
        .distinct()
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("date_id", F.row_number().over(Window.orderBy("full_date")))
    )
    dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_DATE)
    logger.info(f"Created {DIM_DATE} with {dim_date.count()} rows")

    # -------------------------
    # FACT LEDGER
    # -------------------------
    fact_ledger = (
        ledger_df.alias("l")
        .join(spark.table(DIM_ACCOUNT).alias("a"), 
              (F.col("l.Account_no") == F.col("a.Account_no")) &
              (F.col("l.GL_Name") == F.col("a.GL_Name")) &
              (F.col("l.GL_Category") == F.col("a.GL_Category")),
              "left")
        .join(spark.table(DIM_COMPANY).alias("c"), F.col("l.Company_name") == F.col("c.Company_name"), "left")
        .join(spark.table(DIM_DATE).alias("d"), F.col("l.Date") == F.col("d.full_date"), "left")
        .select(
            F.row_number().over(Window.orderBy("l.Account_no", "l.Company_name", "l.Date")).alias("ledger_id"),
            F.col("a.account_id"),
            F.col("c.company_id"),
            F.col("d.date_id"),
            F.col("l.Cost").cast("decimal(18,2)").alias("cost")
        )
    )
    fact_ledger.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FACT_LEDGER)
    logger.info(f"Created {FACT_LEDGER} with {fact_ledger.count()} rows")

    logger.info("✅ Data model build completed successfully")

except Exception as e:
    logger.error("❌ Data model build failed", exc_info=True)
    raise
