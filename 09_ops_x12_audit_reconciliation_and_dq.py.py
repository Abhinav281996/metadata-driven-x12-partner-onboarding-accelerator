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
# 09_ops_x12_audit_reconciliation_and_dq.py
# Step 5: compute metrics, reconciliation checks, and DQ rule execution
#
# Run at the end of your daily job after Step 4 (Gold build).
#
from pyspark.sql import functions as F

CATALOG = "artha_serverless_usa"

PIPELINE_NAME = "x12_837_onboarding"
try:
    TRIGGERED_BY = dbutils.widgets.get("triggered_by")
except Exception:
    TRIGGERED_BY = "manual"

BRONZE = f"{CATALOG}.bronze_x12.bronze_837_raw"
SILVER = f"{CATALOG}.silver_x12.silver_837_claim"

GOLD_H = f"{CATALOG}.gold_claims.claim_header"
GOLD_L = f"{CATALOG}.gold_claims.claim_service_line"
GOLD_DX = f"{CATALOG}.gold_claims.claim_diagnosis"
GOLD_P = f"{CATALOG}.gold_claims.claim_provider"
GOLD_SUB = f"{CATALOG}.gold_claims.claim_subscriber"
GOLD_PAT = f"{CATALOG}.gold_claims.claim_patient"
GOLD_OP = f"{CATALOG}.gold_claims.claim_other_payer"

AUDIT_RUN = f"{CATALOG}.ops_x12.pipeline_run"
AUDIT_FILE = f"{CATALOG}.ops_x12.file_audit"
RECON_SUM = f"{CATALOG}.ops_x12.recon_summary"
DQ_RULES = f"{CATALOG}.cfg_x12.dq_rules_837"
DQ_RESULTS = f"{CATALOG}.ops_x12.dq_results"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
start_ts = spark.sql("select current_timestamp() as ts").collect()[0]["ts"]
print("PIPELINE_RUN_ID:", run_id)

# 1) Register pipeline run (RUNNING)
spark.createDataFrame([(PIPELINE_NAME, run_id, TRIGGERED_BY, start_ts, None, "RUNNING", None)],
                      "pipeline_name string, run_id string, triggered_by string, start_ts timestamp, end_ts timestamp, status string, notes string")      .createOrReplaceTempView("stg_run")

spark.sql(f"""
MERGE INTO {AUDIT_RUN} t
USING stg_run s
ON t.pipeline_name = s.pipeline_name AND t.run_id = s.run_id
WHEN NOT MATCHED THEN INSERT *
""")

# 2) Upsert file_audit base rows from bronze
df_b = (spark.table(BRONZE)
        .select("vendor_id","file_hash","file_path","file_size_bytes","modification_time","run_id")
        .withColumnRenamed("run_id","bronze_run_id")
        .withColumn("first_seen_ts", F.col("modification_time"))
        .withColumn("last_updated_ts", F.current_timestamp())
        .withColumn("status", F.lit("BRONZE_LOADED"))
        .withColumn("error_stage", F.lit(None).cast("string"))
        .withColumn("error_message", F.lit(None).cast("string"))
       )

df_b.createOrReplaceTempView("stg_file_base")

spark.sql(f"""
MERGE INTO {AUDIT_FILE} t
USING stg_file_base s
ON t.file_hash = s.file_hash
WHEN MATCHED THEN UPDATE SET
  t.vendor_id = s.vendor_id,
  t.file_path = s.file_path,
  t.file_size_bytes = s.file_size_bytes,
  t.last_updated_ts = s.last_updated_ts,
  t.bronze_run_id = s.bronze_run_id
WHEN NOT MATCHED THEN INSERT (
  vendor_id,file_hash,file_path,first_seen_ts,last_updated_ts,file_size_bytes,
  bronze_run_id,silver_run_id,gold_run_id,
  status,error_stage,error_message,
  silver_claim_count,silver_service_line_count,silver_total_claim_amount,
  gold_header_count,gold_service_line_count,gold_dx_count,gold_provider_count,gold_subscriber_count,gold_patient_count,gold_other_payer_count,
  recon_header_match,recon_service_line_match,recon_amount_match,recon_notes
) VALUES (
  s.vendor_id,s.file_hash,s.file_path,s.first_seen_ts,s.last_updated_ts,s.file_size_bytes,
  s.bronze_run_id,NULL,NULL,
  s.status,NULL,NULL,
  NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL
)
""")

# 3) SILVER metrics per file_hash
df_s = spark.table(SILVER).select("vendor_id","file_hash","st_control","transaction_version","claim_amount","service_lines_json")

df_s_metrics = (
    df_s.groupBy("vendor_id","file_hash")
        .agg(
            F.count("*").alias("silver_claim_count"),
            F.sum(F.when(F.col("claim_amount").isNull(), F.lit(0.0)).otherwise(F.col("claim_amount"))).alias("silver_total_claim_amount"),
            F.sum(
                F.when(F.col("service_lines_json").isNull(), F.lit(0))
                 .otherwise(F.size(F.from_json(F.col("service_lines_json"),
                     "array<struct<lx:string,qualifier:string,procedure_code:string,modifiers:array<string>,charge_amount:double,units:double>>"))))
            .alias("silver_service_line_count")
        )
)

# 4) GOLD metrics per file_hash
df_h = spark.table(GOLD_H).select("file_hash","claim_amount")
df_l = spark.table(GOLD_L).select("file_hash")
df_dx = spark.table(GOLD_DX).select("file_hash")
df_p = spark.table(GOLD_P).select("file_hash")
df_sub = spark.table(GOLD_SUB).select("file_hash")
df_pat = spark.table(GOLD_PAT).select("file_hash")
df_op = spark.table(GOLD_OP).select("file_hash")

df_g_metrics = (
    df_h.groupBy("file_hash").agg(
        F.count("*").alias("gold_header_count"),
        F.sum(F.when(F.col("claim_amount").isNull(), F.lit(0.0)).otherwise(F.col("claim_amount"))).alias("gold_total_claim_amount")
    )
    .join(df_l.groupBy("file_hash").count().withColumnRenamed("count","gold_service_line_count"), ["file_hash"], "left")
    .join(df_dx.groupBy("file_hash").count().withColumnRenamed("count","gold_dx_count"), ["file_hash"], "left")
    .join(df_p.groupBy("file_hash").count().withColumnRenamed("count","gold_provider_count"), ["file_hash"], "left")
    .join(df_sub.groupBy("file_hash").count().withColumnRenamed("count","gold_subscriber_count"), ["file_hash"], "left")
    .join(df_pat.groupBy("file_hash").count().withColumnRenamed("count","gold_patient_count"), ["file_hash"], "left")
    .join(df_op.groupBy("file_hash").count().withColumnRenamed("count","gold_other_payer_count"), ["file_hash"], "left")
    .fillna(0, subset=[
        "gold_service_line_count","gold_dx_count","gold_provider_count",
        "gold_subscriber_count","gold_patient_count","gold_other_payer_count"
    ])
)

# 5) Reconciliation + update file_audit
df_join = (
    df_s_metrics.join(df_g_metrics, on="file_hash", how="left")
               .withColumn("recon_header_match", F.col("silver_claim_count") == F.col("gold_header_count"))
               .withColumn("recon_service_line_match", F.col("silver_service_line_count") == F.col("gold_service_line_count"))
               .withColumn("recon_amount_match", F.abs(F.col("silver_total_claim_amount") - F.col("gold_total_claim_amount")) < F.lit(0.01))
               .withColumn("recon_notes", F.concat_ws("; ",
                   F.when(~F.col("recon_header_match"), F.lit("Header count mismatch")).otherwise(F.lit(None)),
                   F.when(~F.col("recon_service_line_match"), F.lit("Service line count mismatch")).otherwise(F.lit(None)),
                   F.when(~F.col("recon_amount_match"), F.lit("Claim amount mismatch")).otherwise(F.lit(None))
               ))
               .withColumn("silver_run_id", F.lit(run_id))
               .withColumn("gold_run_id", F.lit(run_id))
               .withColumn("status", F.when(F.col("recon_header_match") & F.col("recon_service_line_match") & F.col("recon_amount_match"),
                                            F.lit("GOLD_BUILT"))
                                   .otherwise(F.lit("FAILED")))
               .withColumn("last_updated_ts", F.current_timestamp())
)

df_join.createOrReplaceTempView("stg_file_metrics")

spark.sql(f"""
MERGE INTO {AUDIT_FILE} t
USING stg_file_metrics s
ON t.file_hash = s.file_hash
WHEN MATCHED THEN UPDATE SET
  t.silver_run_id = s.silver_run_id,
  t.gold_run_id = s.gold_run_id,
  t.last_updated_ts = s.last_updated_ts,
  t.status = CASE WHEN t.status IN ('QUARANTINED') THEN t.status ELSE s.status END,

  t.silver_claim_count = s.silver_claim_count,
  t.silver_service_line_count = s.silver_service_line_count,
  t.silver_total_claim_amount = s.silver_total_claim_amount,

  t.gold_header_count = s.gold_header_count,
  t.gold_service_line_count = s.gold_service_line_count,
  t.gold_dx_count = s.gold_dx_count,
  t.gold_provider_count = s.gold_provider_count,
  t.gold_subscriber_count = s.gold_subscriber_count,
  t.gold_patient_count = s.gold_patient_count,
  t.gold_other_payer_count = s.gold_other_payer_count,

  t.recon_header_match = s.recon_header_match,
  t.recon_service_line_match = s.recon_service_line_match,
  t.recon_amount_match = s.recon_amount_match,
  t.recon_notes = s.recon_notes
""")

# 6) Recon summary per ST (light)
df_st = (spark.table(SILVER)
           .groupBy("vendor_id","file_hash","st_control","transaction_version")
           .agg(
               F.count("*").alias("parsed_claims"),
               F.sum(F.when(F.col("claim_amount").isNull(), F.lit(0.0)).otherwise(F.col("claim_amount"))).alias("parsed_claim_amt")
           ))

df_h_st = (spark.table(GOLD_H)
           .groupBy("file_hash","st_control","transaction_version")
           .agg(
               F.count("*").alias("gold_headers"),
               F.sum(F.when(F.col("claim_amount").isNull(), F.lit(0.0)).otherwise(F.col("claim_amount"))).alias("header_claim_amt")
           ))

df_l_st = (spark.table(GOLD_L).groupBy("file_hash","st_control").count().withColumnRenamed("count","gold_lines"))

df_recon = (df_st.join(df_h_st, ["file_hash","st_control","transaction_version"], "left")
                 .join(df_l_st, ["file_hash","st_control"], "left")
                 .withColumn("expected_claims", F.lit(None).cast("long"))
                 .withColumn("header_match", F.col("parsed_claims") == F.col("gold_headers"))
                 .withColumn("lines_match", F.col("gold_lines").isNotNull())
                 .withColumn("amount_match", F.abs(F.col("parsed_claim_amt") - F.col("header_claim_amt")) < F.lit(0.01))
                 .withColumn("load_ts", F.current_timestamp())
          )

df_recon.createOrReplaceTempView("stg_recon")

spark.sql(f"""
MERGE INTO {RECON_SUM} t
USING stg_recon s
ON t.file_hash = s.file_hash AND t.st_control = s.st_control
WHEN NOT MATCHED THEN INSERT *
""")

# 7) DQ rules execution (metadata-driven)
rules = (spark.table(DQ_RULES)
         .filter(F.col("active_flag") == True)
         .select("rule_id","entity","severity","vendor_id","sql_predicate")
         .collect())

ENTITY_VIEW = {
  "SILVER_CLAIM": "v_silver_claim",
  "GOLD_HEADER": "v_gold_header",
  "GOLD_SERVICE_LINE": "v_gold_service_line",
  "GOLD_DIAGNOSIS": "v_gold_diagnosis",
  "GOLD_PROVIDER": "v_gold_provider",
}

spark.table(SILVER).createOrReplaceTempView("v_silver_claim")
spark.table(GOLD_H).createOrReplaceTempView("v_gold_header")
spark.table(GOLD_L).createOrReplaceTempView("v_gold_service_line")
spark.table(GOLD_DX).createOrReplaceTempView("v_gold_diagnosis")
spark.table(GOLD_P).createOrReplaceTempView("v_gold_provider")

dq_dfs = []
for r in rules:
    view = ENTITY_VIEW.get(r["entity"])
    if not view:
        continue
    vendor_filter = f" AND vendor_id = '{r['vendor_id']}' " if r["vendor_id"] else ""
    sql = f"""
      SELECT
        '{PIPELINE_NAME}' as pipeline_name,
        '{run_id}' as run_id,
        vendor_id,
        file_hash,
        '{r['rule_id']}' as rule_id,
        '{r['entity']}' as entity,
        '{r['severity']}' as severity,
        COUNT(1) as failed_count,
        substring(to_json(collect_list(struct(*))), 1, 2000) as sample_values,
        current_timestamp() as load_ts
      FROM {view}
      WHERE ({r['sql_predicate']}) {vendor_filter}
      GROUP BY vendor_id, file_hash
    """
    dq_dfs.append(spark.sql(sql))

if dq_dfs:
    df_dq = dq_dfs[0]
    for d in dq_dfs[1:]:
        df_dq = df_dq.unionByName(d, allowMissingColumns=True)
    df_dq.createOrReplaceTempView("stg_dq")
    spark.sql(f"""
    MERGE INTO {DQ_RESULTS} t
    USING stg_dq s
    ON t.pipeline_name = s.pipeline_name AND t.run_id = s.run_id AND t.rule_id = s.rule_id
       AND t.file_hash = s.file_hash AND t.vendor_id = s.vendor_id
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("✅ DQ results rows:", df_dq.count())
else:
    print("No active DQ rules found. Skipping DQ execution.")

# 8) Finalize pipeline run status
has_fail = spark.table(AUDIT_FILE).filter(F.col("status") == "FAILED").limit(1).count() > 0
end_ts = spark.sql("select current_timestamp() as ts").collect()[0]["ts"]
final_status = "FAILED" if has_fail else "SUCCESS"
notes = "One or more files failed reconciliation." if has_fail else "All files reconciled."

spark.createDataFrame([(PIPELINE_NAME, run_id, end_ts, final_status, notes)],
                      "pipeline_name string, run_id string, end_ts timestamp, status string, notes string")      .createOrReplaceTempView("stg_run_final")

spark.sql(f"""
MERGE INTO {AUDIT_RUN} t
USING stg_run_final s
ON t.pipeline_name = s.pipeline_name AND t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
  t.end_ts = s.end_ts,
  t.status = s.status,
  t.notes = s.notes
""")

print(f"✅ Step 5 complete. Run status: {final_status}")