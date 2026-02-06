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

# 04_gold_claims_837_normalize_service_lines.py
# Explode Silver service_lines_json into Gold claim_service_line (1 row per service line)
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType

CATALOG = "artha_serverless_usa"
SILVER_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
GOLD_TBL = f"{CATALOG}.gold_claims.claim_service_line"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

# Schema for the JSON we produced in Step 3
svc_schema = ArrayType(StructType([
    StructField("lx", StringType(), True),
    StructField("qualifier", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("modifiers", ArrayType(StringType()), True),
    StructField("charge_amount", DoubleType(), True),
    StructField("units", DoubleType(), True),
]))

df = spark.table(SILVER_TBL)

df_lines = (
    df
    .withColumn("run_id", F.lit(run_id))
    .withColumn("load_ts", F.current_timestamp())
    .withColumn("service_lines", F.from_json(F.col("service_lines_json"), svc_schema))
    .withColumn("service_line", F.explode_outer(F.col("service_lines")))
    .withColumn("lx", F.col("service_line.lx"))
    .withColumn("procedure_qualifier", F.col("service_line.qualifier"))
    .withColumn("procedure_code", F.col("service_line.procedure_code"))
    .withColumn("modifiers", F.col("service_line.modifiers"))
    .withColumn("modifiers_str", F.concat_ws(",", F.col("modifiers")))
    .withColumn("charge_amount", F.col("service_line.charge_amount").cast("double"))
    .withColumn("units", F.col("service_line.units").cast("double"))
    .withColumn("service_line_key",
                F.sha2(F.concat_ws("||",
                                   F.col("vendor_id"),
                                   F.col("file_hash"),
                                   F.coalesce(F.col("st_control"), F.lit("")),
                                   F.coalesce(F.col("claim_id"), F.lit("")),
                                   F.coalesce(F.col("lx"), F.lit("")),
                                   F.coalesce(F.col("procedure_code"), F.lit("")),
                                   F.coalesce(F.col("modifiers_str"), F.lit(""))), 256))
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
        "lx",
        "procedure_qualifier",
        "procedure_code",
        "modifiers_str",
        "charge_amount",
        "units",
        "service_line_key"
    )
)

# Filter out rows where there were no service lines (optional)
df_lines = df_lines.filter(F.col("procedure_code").isNotNull())

df_lines.createOrReplaceTempView("stg_claim_lines")

# Idempotent MERGE (unique by file_hash + st_control + claim_id + lx + procedure_code + modifiers)
spark.sql(f"""
MERGE INTO {GOLD_TBL} t
USING stg_claim_lines s
ON  t.file_hash = s.file_hash
AND t.st_control <=> s.st_control
AND t.claim_id   <=> s.claim_id
AND t.lx         <=> s.lx
AND t.procedure_code <=> s.procedure_code
AND t.modifiers_str  <=> s.modifiers_str
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Gold claim_service_line updated.")
