# Databricks notebook source
# DBTITLE 1,Cell 1
# 10_orchestrator_x12_837_vendor_driven.py
# Step 6: Metadata-driven orchestration for X12 837 onboarding
#
# What this does:
# - Reads cfg_x12.vendor_registry (active vendors)
# - Discovers new files per vendor by listing landing_path and comparing to ops_x12.file_audit
# - Runs the configured notebooks in cfg_x12.pipeline_steps (with vendor overrides) via dbutils.notebook.run
# - Updates cfg_x12.vendor_state with last_success_ts and run_id
#
# IMPORTANT:
# - You must update cfg_x12.pipeline_steps.notebook_path to match your workspace paths.
# - Your Step 1 (BRONZE) notebook should accept optional params:
#     vendor_id, landing_path, file_paths_json
#
import json
import re
from pyspark.sql import functions as F

CATALOG = "artha_serverless_usa"
PIPELINE = "x12_837_onboarding"

VENDOR_REG = f"{CATALOG}.cfg_x12.vendor_registry"
VENDOR_STATE = f"{CATALOG}.cfg_x12.vendor_state"
STEPS_TBL = f"{CATALOG}.cfg_x12.pipeline_steps"
OVR_TBL = f"{CATALOG}.cfg_x12.vendor_step_overrides"
FILE_AUDIT = f"{CATALOG}.ops_x12.file_audit"

# Widgets (optional)
try:
    dbutils.widgets.text("vendor_id", "")
    dbutils.widgets.text("triggered_by", "job")
except Exception:
    pass

vendor_filter = ""
try:
    vendor_filter = dbutils.widgets.get("vendor_id").strip()
except Exception:
    vendor_filter = ""

triggered_by = "manual"
try:
    triggered_by = dbutils.widgets.get("triggered_by").strip() or "manual"
except Exception:
    pass

master_run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("MASTER_RUN_ID:", master_run_id, "| triggered_by:", triggered_by)

# Helper: safe JSON
def _loads(s, default):
    if s is None:
        return default
    s = s.strip()
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default

# 1) Read active vendors
df_v = spark.table(VENDOR_REG).filter(F.col("active_flag") == True)
if vendor_filter:
    df_v = df_v.filter(F.col("vendor_id") == vendor_filter)

vendors = df_v.select("vendor_id","landing_path","file_name_regex","max_file_size_mb","quarantine_on_oversize").collect()
if not vendors:
    print("No active vendors found. Nothing to do.")
    dbutils.notebook.exit("NO_VENDORS")

# 2) Read default pipeline steps
df_steps = (spark.table(STEPS_TBL)
            .filter((F.col("pipeline_name") == PIPELINE) & (F.col("enabled_flag") == True))
            .select("step_order","step_name","notebook_path","timeout_seconds","default_params_json")
            .orderBy("step_order"))

steps = df_steps.collect()
if not steps:
    raise Exception("No enabled steps found in cfg_x12.pipeline_steps")

# 3) Read overrides once
df_ovr = (spark.table(OVR_TBL)
          .filter(F.col("pipeline_name") == PIPELINE)
          .select("vendor_id","step_name","enabled_override","params_override_json"))

ovr = {(r["vendor_id"], r["step_name"]): r for r in df_ovr.collect()}

# 4) Discover files for a vendor
def discover_files(landing_path: str, regex_pat: str):
    try:
        files = dbutils.fs.ls(landing_path)
    except Exception as e:
        print(f"❌ Cannot list landing_path {landing_path}: {e}")
        return []

    rx = re.compile(regex_pat) if regex_pat else None

    out = []
    for f in files:
        if f.isDir():
            continue
        p = f.path
        if rx and not rx.match(p):
            continue
        out.append({"path": p, "size": int(getattr(f, "size", 0) or 0)})
    return out

# 5) Get already-built file hashes from ops table (optional skip)
# NOTE: We skip by file_path (since hashing here would require reading file content again).
# This is enough for POC; production can use file_hash from bronze.
df_done = (spark.table(FILE_AUDIT)
           .filter(F.col("status") == "GOLD_BUILT")
           .select("vendor_id","file_path")
           .dropDuplicates())
done = {(r["vendor_id"], r["file_path"]) for r in df_done.collect()}

# 6) Execute pipeline per vendor
for v in vendors:
    vendor_id = v["vendor_id"]
    landing_path = v["landing_path"]
    regex_pat = v["file_name_regex"]
    max_mb = v["max_file_size_mb"] or 0
    quarantine = bool(v["quarantine_on_oversize"])

    print("\n==============================")
    print("Vendor:", vendor_id)
    print("Landing:", landing_path)
    print("==============================")

    files = discover_files(landing_path, regex_pat)

    # Skip already GOLD_BUILT paths
    new_files = [f for f in files if (vendor_id, f["path"]) not in done]

    if not new_files:
        print("✅ No new files found. Skipping vendor.")
        continue

    # Quarantine oversize (POC): just exclude from processing list
    keep_files = []
    for f in new_files:
        size_mb = (f["size"] / (1024 * 1024)) if f["size"] else 0
        if quarantine and max_mb and size_mb > max_mb:
            print(f"⚠️ Oversize file excluded (quarantine): {f['path']} size_mb={size_mb:.1f}")
            continue
        keep_files.append(f["path"])

    if not keep_files:
        print("No processable files after quarantine. Skipping vendor.")
        continue

    file_paths_json = json.dumps(keep_files)

    # Run each configured step in order
    for s in steps:
        step_name = s["step_name"]
        nb_path = s["notebook_path"]
        timeout = int(s["timeout_seconds"] or 3600)

        # Apply overrides
        o = ovr.get((vendor_id, step_name))
        enabled = True
        params = _loads(s["default_params_json"], {})
        if o:
            if o["enabled_override"] is not None:
                enabled = bool(o["enabled_override"])
            params.update(_loads(o["params_override_json"], {}))

        if not enabled:
            print(f"⏭️  Skipping step {step_name} (disabled by override)")
            continue

        # Standard args passed to every notebook
        args = {
            "vendor_id": vendor_id,
            "landing_path": landing_path,
            "file_paths_json": file_paths_json,
            "master_run_id": master_run_id,
            "triggered_by": triggered_by,
        }
        # Merge extra params from config
        args.update(params)

        print(f"▶️ Running {step_name}: {nb_path}")
        try:
            res = dbutils.notebook.run(nb_path, timeout, args)
            print(f"✅ {step_name} done. Result: {res}")
        except Exception as e:
            print(f"❌ Step failed: {step_name} | {e}")
            # Update vendor_state as FAILED
            current_ts = spark.sql("select current_timestamp() as ts").collect()[0]["ts"]
            spark.createDataFrame([(vendor_id, None, None, "FAILED", master_run_id, str(e)[:500], current_ts)],
                                  "vendor_id string, last_discovery_ts timestamp, last_success_ts timestamp, last_status string, last_run_id string, notes string, updated_ts timestamp")                  .createOrReplaceTempView("stg_vs_fail")
            spark.sql(f"""
              MERGE INTO {VENDOR_STATE} t
              USING stg_vs_fail s
              ON t.vendor_id = s.vendor_id
              WHEN MATCHED THEN UPDATE SET
                t.last_status = s.last_status,
                t.last_run_id = s.last_run_id,
                t.notes = s.notes,
                t.updated_ts = current_timestamp()
              WHEN NOT MATCHED THEN INSERT *
            """)
            raise

    # If all steps succeeded, update vendor_state SUCCESS
    current_ts = spark.sql("select current_timestamp() as ts").collect()[0]["ts"]
    spark.createDataFrame([(vendor_id, current_ts, current_ts, "SUCCESS", master_run_id, None, current_ts)],
                          "vendor_id string, last_discovery_ts timestamp, last_success_ts timestamp, last_status string, last_run_id string, notes string, updated_ts timestamp")          .createOrReplaceTempView("stg_vs_ok")

    spark.sql(f"""
      MERGE INTO {VENDOR_STATE} t
      USING stg_vs_ok s
      ON t.vendor_id = s.vendor_id
      WHEN MATCHED THEN UPDATE SET
        t.last_discovery_ts = s.last_discovery_ts,
        t.last_success_ts = s.last_success_ts,
        t.last_status = s.last_status,
        t.last_run_id = s.last_run_id,
        t.notes = s.notes,
        t.updated_ts = current_timestamp()
      WHEN NOT MATCHED THEN INSERT *
    """)
    print("✅ Vendor completed:", vendor_id)

print("\n🎉 Step 6 orchestration complete.")
dbutils.notebook.exit("SUCCESS")