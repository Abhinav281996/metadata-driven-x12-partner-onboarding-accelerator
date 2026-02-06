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

# DBTITLE 1,Cell 1
# 08_gold_claims_837_normalize_other_payer.py
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
GOLD_TBL = f"{CATALOG}.gold_claims.claim_other_payer"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

payer_schema = ArrayType(StructType([
    StructField("payer_name", StringType(), True),
    StructField("payer_id", StringType(), True),
]))

df = spark.table(SILVER_TBL)

df_payer = (
    df.withColumn("run_id", F.lit(run_id))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("claim_key", F.sha2(F.concat_ws("||",
          F.col("vendor_id"), F.col("file_hash"),
          F.coalesce(F.col("st_control"), F.lit("")),
          F.coalesce(F.col("claim_id"), F.lit(""))
      ), 256))
      .withColumn("payer_arr", F.from_json(F.col("other_payer_json"), payer_schema))
      .select("*", F.posexplode_outer(F.col("payer_arr")).alias("payer_pos", "payer_col"))
      .withColumn("payer_seq", (F.col("payer_pos") + F.lit(1)).cast("int"))
      .withColumn("payer_name", F.col("payer_col.payer_name"))
      .withColumn("payer_id", F.col("payer_col.payer_id"))
      .withColumn("other_payer_key", F.sha2(F.concat_ws("||",
          F.col("claim_key"),
          F.coalesce(F.col("payer_seq").cast("string"), F.lit("")),
          F.coalesce(F.col("payer_id"), F.lit("")),
          F.coalesce(F.col("payer_name"), F.lit(""))
      ), 256))
      .select(
          "vendor_id","file_hash","file_path","run_id","parse_ts","load_ts",
          "isa_control","gs_control","st_control","transaction_version",
          "claim_id","claim_key",
          "payer_seq","payer_name","payer_id",
          "other_payer_key"
      )
      .filter(F.col("payer_name").isNotNull() | F.col("payer_id").isNotNull())
)

df_payer.createOrReplaceTempView("stg_other_payer")

spark.sql(f"""
MERGE INTO {GOLD_TBL} t
USING stg_other_payer s
ON t.other_payer_key = s.other_payer_key
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_other_payer updated.")