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
from pyspark.sql import functions as F

# -----------------------------
# PARAMETERS
# -----------------------------
run_id = spark.sql("select uuid() as id").collect()[0]["id"]

landing_root = "dbfs:/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837"
input_path = f"{landing_root}/*/*"   # vendor folders, no dt partition

print("RUN_ID:", run_id)
print("INPUT:", input_path)

# -----------------------------
# READ FILES (1 row per file)
# -----------------------------
df_files = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(input_path)
)

# binaryFile columns: path, modificationTime, length, content
df = (
    df_files
    .withColumn("file_path", F.col("path"))
    .withColumn("modification_time", F.col("modificationTime"))
    .withColumn("file_size_bytes", F.col("length"))
    .withColumn("vendor_id", F.regexp_extract(F.col("path"), r"/837/([^/]+)/", 1))  # vendorA/vendorB
    .withColumn("run_id", F.lit(run_id))
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("file_hash", F.sha2(F.col("content"), 256))
    .withColumn("content_str", F.decode(F.col("content"), "UTF-8"))
    # basic X12 check: file typically starts with ISA
    .withColumn("is_x12_like", F.expr("content_str is not null AND instr(content_str, 'ISA') = 1"))
    .select(
        "vendor_id", "run_id",
        "file_path", "modification_time", "file_size_bytes",
        "file_hash", "content_str", "is_x12_like", "ingest_ts"
    )
)

print("Files detected:", df.count())
display(df.select("vendor_id", "file_path", "file_size_bytes", "modification_time"))

# -----------------------------
# SPLIT GOOD vs QUARANTINE
# -----------------------------
df_good = df.filter(F.col("is_x12_like") == True)
df_bad  = (
    df.filter(F.col("is_x12_like") == False)
      .withColumn("reject_reason", F.lit("Not X12-like (ISA not at position 1)"))
      .select("vendor_id", "run_id", "file_path", "modification_time", "file_size_bytes",
              "file_hash", "content_str", "reject_reason", "ingest_ts")
)

# -----------------------------
# MERGE GOOD (IDEMPOTENT)
# -----------------------------
df_good.createOrReplaceTempView("stg_good")

spark.sql("""
MERGE INTO artha_serverless_usa.bronze_x12.bronze_837_raw t
USING stg_good s
ON t.file_hash = s.file_hash
WHEN NOT MATCHED THEN INSERT *
""")

# -----------------------------
# WRITE QUARANTINE
# (can also MERGE if you want strict idempotency)
# -----------------------------
df_bad.write.mode("append").format("delta").saveAsTable("artha_serverless_usa.bronze_x12.bronze_837_quarantine")

print("Loaded:", df_good.count(), "Rejected:", df_bad.count())