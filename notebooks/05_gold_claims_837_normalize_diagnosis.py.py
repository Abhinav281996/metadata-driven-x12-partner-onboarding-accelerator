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

# DBTITLE 1,Cell 2
# 05_gold_claims_837_normalize_diagnosis.py
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
GOLD_TBL = f"{CATALOG}.gold_claims.claim_diagnosis"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

dx_schema = ArrayType(StructType([
    StructField("diag_qualifier", StringType(), True),
    StructField("diag_code", StringType(), True),
    StructField("raw", StringType(), True),
]))

df = spark.table(SILVER_TBL)

df_dx = (
    df.withColumn("run_id", F.lit(run_id))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("claim_key", F.sha2(F.concat_ws("||",
          F.col("vendor_id"), F.col("file_hash"),
          F.coalesce(F.col("st_control"), F.lit("")),
          F.coalesce(F.col("claim_id"), F.lit(""))
      ), 256))
      .withColumn("dx_arr", F.from_json(F.col("diagnosis_json"), dx_schema))
      .select("*", F.posexplode_outer(F.col("dx_arr")).alias("dx_pos", "dx_col"))
      .withColumn("diag_seq", (F.col("dx_pos") + F.lit(1)).cast("int"))
      .withColumn("diag_qualifier", F.col("dx_col.diag_qualifier"))
      .withColumn("diag_code", F.col("dx_col.diag_code"))
      .withColumn("diag_raw", F.col("dx_col.raw"))
      .withColumn("diagnosis_key", F.sha2(F.concat_ws("||",
          F.col("claim_key"),
          F.coalesce(F.col("diag_seq").cast("string"), F.lit("")),
          F.coalesce(F.col("diag_qualifier"), F.lit("")),
          F.coalesce(F.col("diag_code"), F.lit(""))
      ), 256))
      .select(
          "vendor_id","file_hash","file_path","run_id","parse_ts","load_ts",
          "isa_control","gs_control","st_control","transaction_version",
          "claim_id","claim_key",
          "diag_seq","diag_qualifier","diag_code","diag_raw",
          "diagnosis_key"
      )
      .filter(F.col("diag_code").isNotNull())
)

df_dx.createOrReplaceTempView("stg_dx")

spark.sql(f"""
MERGE INTO {GOLD_TBL} t
USING stg_dx s
ON t.diagnosis_key = s.diagnosis_key
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_diagnosis updated.")