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

# 07_gold_claims_837_normalize_subscriber_patient.py
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
SUB_TBL = f"{CATALOG}.gold_claims.claim_subscriber"
PAT_TBL = f"{CATALOG}.gold_claims.claim_patient"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

party_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("addr1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
])

df = spark.table(SILVER_TBL)

base_df = (
    df.withColumn("run_id", F.lit(run_id))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("claim_key", F.sha2(F.concat_ws("||",
          F.col("vendor_id"), F.col("file_hash"),
          F.coalesce(F.col("st_control"), F.lit("")),
          F.coalesce(F.col("claim_id"), F.lit(""))
      ), 256))
)

# Subscriber
sub = (
    base_df.withColumn("sub", F.from_json(F.col("subscriber_json"), party_schema))
      .withColumn("member_id", F.col("sub.member_id"))
      .withColumn("first_name", F.col("sub.first_name"))
      .withColumn("last_name", F.col("sub.last_name"))
      .withColumn("dob", F.to_date(F.when(F.length(F.col("sub.dob"))==8, F.col("sub.dob")), "yyyyMMdd"))
      .withColumn("gender", F.col("sub.gender"))
      .withColumn("addr1", F.col("sub.addr1"))
      .withColumn("city", F.col("sub.city"))
      .withColumn("state", F.col("sub.state"))
      .withColumn("zip", F.col("sub.zip"))
      .withColumn("subscriber_key", F.sha2(F.concat_ws("||",
          F.col("claim_key"), F.coalesce(F.col("member_id"), F.lit("")), F.lit("SUB")
      ), 256))
      .select(
          "vendor_id","file_hash","file_path","run_id","parse_ts","load_ts",
          "isa_control","gs_control","st_control","transaction_version",
          "claim_id","claim_key",
          "member_id","first_name","last_name","dob","gender","addr1","city","state","zip",
          "subscriber_key"
      )
      .filter(F.col("member_id").isNotNull())
)
sub.createOrReplaceTempView("stg_sub")

spark.sql(f"""
MERGE INTO {SUB_TBL} t
USING stg_sub s
ON t.subscriber_key = s.subscriber_key
WHEN NOT MATCHED THEN INSERT *
""")

# Patient
pat = (
    base_df.withColumn("pat", F.from_json(F.col("patient_json"), party_schema))
      .withColumn("member_id", F.col("pat.member_id"))
      .withColumn("first_name", F.col("pat.first_name"))
      .withColumn("last_name", F.col("pat.last_name"))
      .withColumn("dob", F.to_date(F.when(F.length(F.col("pat.dob"))==8, F.col("pat.dob")), "yyyyMMdd"))
      .withColumn("gender", F.col("pat.gender"))
      .withColumn("addr1", F.col("pat.addr1"))
      .withColumn("city", F.col("pat.city"))
      .withColumn("state", F.col("pat.state"))
      .withColumn("zip", F.col("pat.zip"))
      .withColumn("patient_key", F.sha2(F.concat_ws("||",
          F.col("claim_key"), F.coalesce(F.col("member_id"), F.lit("")), F.lit("PAT")
      ), 256))
      .select(
          "vendor_id","file_hash","file_path","run_id","parse_ts","load_ts",
          "isa_control","gs_control","st_control","transaction_version",
          "claim_id","claim_key",
          "member_id","first_name","last_name","dob","gender","addr1","city","state","zip",
          "patient_key"
      )
      .filter(F.col("member_id").isNotNull())
)
pat.createOrReplaceTempView("stg_pat")

spark.sql(f"""
MERGE INTO {PAT_TBL} t
USING stg_pat s
ON t.patient_key = s.patient_key
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_subscriber + claim_patient updated.")
