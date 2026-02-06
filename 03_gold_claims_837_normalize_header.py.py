# Databricks notebook source
# Patch snippet (add to the TOP of each step notebook)
# Makes notebooks callable from orchestrator with parameters.
#
# In each notebook, add these widgets and read them safely:
try:
    dbutils.widgets.text("vendor_id", "")
    dbutils.widgets.text("landing_path", "")
    dbutils.widgets.text("file_paths_json", "")
    dbutils.widgets.text("master_run_id", "")
    dbutils.widgets.text("triggered_by", "")
except Exception:
    pass

vendor_id = dbutils.widgets.get("vendor_id").strip() or None
landing_path = dbutils.widgets.get("landing_path").strip() or None
file_paths_json = dbutils.widgets.get("file_paths_json").strip() or None
master_run_id = dbutils.widgets.get("master_run_id").strip() or None

# For Step-1 bronze ingest:
# - If file_paths_json exists, read only those files; else read everything from landing_path
# Example:
import json
paths = json.loads(file_paths_json) if file_paths_json else None


# COMMAND ----------

# 03_gold_claims_837_normalize_header.py
# Normalize Silver (1 row per claim) into Gold claim_header (analytics/downstream-ready)
from pyspark.sql import functions as F

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
GOLD_TBL = f"{CATALOG}.gold_claims.claim_header"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

# Read Silver claims
df = spark.table(SILVER_TBL)

# Basic normalization + typing
df_hdr = (
    df
    .withColumn("run_id", F.lit(run_id))
    .withColumn("load_ts", F.current_timestamp())
    # Convert yyyymmdd strings to DATE (safe)
    .withColumn("claim_from_date",
                F.to_date(F.when(F.length("claim_from_dt")==8, F.col("claim_from_dt")), "yyyyMMdd"))
    .withColumn("claim_to_date",
                F.to_date(F.when(F.length("claim_to_dt")==8, F.col("claim_to_dt")), "yyyyMMdd"))
    .withColumn("claim_key",
                F.sha2(F.concat_ws("||",
                                   F.col("vendor_id"),
                                   F.col("file_hash"),
                                   F.coalesce(F.col("st_control"), F.lit("")),
                                   F.coalesce(F.col("claim_id"), F.lit(""))), 256))
    .select(
        "vendor_id",
        "file_hash",
        "file_path",
        "run_id",
        "parse_ts",
        "load_ts",
        "isa_control",
        "gs_control",
        "st_control",
        F.col("transaction_version").alias("transaction_version"),
        "claim_id",
        F.col("claim_amount").cast("double").alias("claim_amount"),
        "place_of_service",
        F.col("subscriber_id").alias("subscriber_member_id"),
        F.col("patient_id").alias("patient_member_id"),
        "billing_npi",
        "rendering_npi",
        "claim_from_date",
        "claim_to_date",
        "claim_key"
    )
)

df_hdr.createOrReplaceTempView("stg_claim_header")

# Idempotent MERGE (unique claim per file_hash + st_control + claim_id)
spark.sql(f"""
MERGE INTO {GOLD_TBL} t
USING stg_claim_header s
ON  t.file_hash = s.file_hash
AND t.st_control <=> s.st_control
AND t.claim_id   <=> s.claim_id
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_header updated.")
