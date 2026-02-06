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

# 06_gold_claims_837_normalize_providers.py
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
GOLD_TBL = f"{CATALOG}.gold_claims.claim_provider"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

prov_schema = ArrayType(StructType([
    StructField("role", StringType(), True),
    StructField("nm1_code", StringType(), True),
    StructField("entity_type", StringType(), True),
    StructField("last_name_or_org", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("id_qualifier", StringType(), True),
    StructField("id_value", StringType(), True),
    StructField("npi", StringType(), True),
    StructField("addr1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
]))

df = spark.table(SILVER_TBL)

df_p = (
    df.withColumn("run_id", F.lit(run_id))
      .withColumn("load_ts", F.current_timestamp())
      .withColumn("claim_key", F.sha2(F.concat_ws("||",
          F.col("vendor_id"), F.col("file_hash"),
          F.coalesce(F.col("st_control"), F.lit("")),
          F.coalesce(F.col("claim_id"), F.lit(""))
      ), 256))
      .withColumn("prov_arr", F.from_json(F.col("providers_json"), prov_schema))
      .withColumn("prov", F.explode_outer(F.col("prov_arr")))
      .withColumn("provider_role", F.col("prov.role"))
      .withColumn("nm1_code", F.col("prov.nm1_code"))
      .withColumn("entity_type", F.col("prov.entity_type"))
      .withColumn("last_name_or_org", F.col("prov.last_name_or_org"))
      .withColumn("first_name", F.col("prov.first_name"))
      .withColumn("id_qualifier", F.col("prov.id_qualifier"))
      .withColumn("id_value", F.col("prov.id_value"))
      .withColumn("npi", F.col("prov.npi"))
      .withColumn("addr1", F.col("prov.addr1"))
      .withColumn("city", F.col("prov.city"))
      .withColumn("state", F.col("prov.state"))
      .withColumn("zip", F.col("prov.zip"))
      .withColumn("provider_key", F.sha2(F.concat_ws("||",
          F.col("claim_key"),
          F.coalesce(F.col("provider_role"), F.lit("")),
          F.coalesce(F.col("npi"), F.lit("")),
          F.coalesce(F.col("id_value"), F.lit("")),
          F.coalesce(F.col("last_name_or_org"), F.lit(""))
      ), 256))
      .select(
          "vendor_id","file_hash","file_path","run_id","parse_ts","load_ts",
          "isa_control","gs_control","st_control","transaction_version",
          "claim_id","claim_key",
          "provider_role","nm1_code","entity_type","last_name_or_org","first_name",
          "id_qualifier","id_value","npi",
          "addr1","city","state","zip",
          "provider_key"
      )
      .filter(F.col("provider_role").isNotNull())
)

df_p.createOrReplaceTempView("stg_provider")

spark.sql(f"""
MERGE INTO {GOLD_TBL} t
USING stg_provider s
ON t.provider_key = s.provider_key
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_provider updated.")
