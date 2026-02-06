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

import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

CATALOG = "artha_serverless_usa"
BRONZE_TBL = f"{CATALOG}.bronze_x12.bronze_837_raw"
SILVER_CLAIM_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
SILVER_ERR_TBL = f"{CATALOG}.silver_x12.silver_837_parse_error"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

# -----------------------------
# 1) Read only NEW bronze files
# -----------------------------
df_bronze = spark.table(BRONZE_TBL).select(
    "vendor_id","file_hash","file_path","run_id","content_str","modification_time","file_size_bytes"
)

df_processed_hashes = (
    spark.table(SILVER_CLAIM_TBL).select("file_hash").distinct()
    .union(spark.table(SILVER_ERR_TBL).select("file_hash").distinct())
    .distinct()
)

df_new = df_bronze.join(df_processed_hashes, on="file_hash", how="left_anti")

print("New files to parse:", df_new.count())
display(df_new.select("vendor_id","file_path","file_size_bytes","modification_time"))

# -----------------------------
# 2) Parser helpers (POC)
# -----------------------------
def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def parse_837_file(vendor_id, file_hash, file_path, bronze_run_id, content_str):
    """
    Returns a list of dict rows.
    If parsing fails, returns one FAIL row (we'll route to error table).
    """
    parse_ts = pd.Timestamp.utcnow()

    if content_str is None or not content_str.startswith("ISA"):
        return [{
            "vendor_id": vendor_id,
            "file_hash": file_hash,
            "file_path": file_path,
            "run_id": bronze_run_id,
            "parse_ts": parse_ts,
            "isa_control": None,
            "gs_control": None,
            "st_control": None,
            "transaction_version": None,
            "claim_id": None,
            "claim_amount": None,
            "place_of_service": None,
            "subscriber_id": None,
            "patient_id": None,
            "billing_npi": None,
            "rendering_npi": None,
            "claim_from_dt": None,
            "claim_to_dt": None,
            "service_lines_json": None,
            "claim_block_text": None,
            "parse_status": "FAIL",
            "error_stage": "basic_check",
            "error_message": "Content is null or does not start with ISA"
        }]

    # separators (POC): detect element sep from 4th char; segment sep by first "~" seen early
    elem_sep = content_str[3]  # ISA* => "*"
    seg_sep = "~" if "~" in content_str[:5000] else "\n"

    segments = [s.strip() for s in content_str.split(seg_sep) if s and s.strip()]
    if not segments:
        return [{
            "vendor_id": vendor_id, "file_hash": file_hash, "file_path": file_path,
            "run_id": bronze_run_id, "parse_ts": parse_ts,
            "isa_control": None, "gs_control": None, "st_control": None,
            "transaction_version": None, "claim_id": None, "claim_amount": None,
            "place_of_service": None, "subscriber_id": None, "patient_id": None,
            "billing_npi": None, "rendering_npi": None, "claim_from_dt": None, "claim_to_dt": None,
            "service_lines_json": None, "claim_block_text": None,
            "parse_status": "FAIL", "error_stage": "split_segments",
            "error_message": "No segments after splitting"
        }]

    # envelope controls
    isa_control = None
    gs_control = None

    try:
        isa = next(s for s in segments if s.startswith("ISA"+elem_sep))
        isa_parts = isa.split(elem_sep)
        # ISA13 is index 13 (0=ISA)
        isa_control = isa_parts[13] if len(isa_parts) > 13 else None
    except Exception:
        pass

    try:
        gs = next(s for s in segments if s.startswith("GS"+elem_sep))
        gs_parts = gs.split(elem_sep)
        # GS06 is index 6
        gs_control = gs_parts[6] if len(gs_parts) > 6 else None
    except Exception:
        pass

    # grab some file-level context (billing provider, subscriber, patient)
    billing_npi = None
    subscriber_id = None
    patient_id = None

    for s in segments:
        if s.startswith("NM1"+elem_sep+"85"+elem_sep):  # billing provider
            p = s.split(elem_sep)
            # look for XX qualifier and take the last element as NPI (common)
            if "XX" in p:
                billing_npi = p[-1]
        if s.startswith("NM1"+elem_sep+"IL"+elem_sep):  # subscriber
            p = s.split(elem_sep)
            # usually last element is member id after MI*
            if len(p) >= 10:
                subscriber_id = p[-1]
        if s.startswith("NM1"+elem_sep+"QC"+elem_sep):  # patient
            p = s.split(elem_sep)
            if len(p) >= 10:
                patient_id = p[-1]

    if patient_id is None:
        patient_id = subscriber_id

    # parse each ST..SE 837 transaction, then split claims by CLM
    rows = []
    i = 0
    found_837 = False

    while i < len(segments):
        seg = segments[i]
        if seg.startswith("ST"+elem_sep):
            st_parts = seg.split(elem_sep)
            if len(st_parts) > 1 and st_parts[1] == "837":
                found_837 = True
                st_control = st_parts[2] if len(st_parts) > 2 else None
                txn_version = st_parts[3] if len(st_parts) > 3 else None

                # find SE for this transaction
                j = i + 1
                while j < len(segments) and not segments[j].startswith("SE"+elem_sep):
                    j += 1
                txn_segs = segments[i:(j+1 if j < len(segments) else len(segments))]

                # within txn, find claims (CLM)
                clm_idx = [k for k, s in enumerate(txn_segs) if s.startswith("CLM"+elem_sep)]
                if not clm_idx:
                    rows.append({
                        "vendor_id": vendor_id, "file_hash": file_hash, "file_path": file_path,
                        "run_id": bronze_run_id, "parse_ts": parse_ts,
                        "isa_control": isa_control, "gs_control": gs_control, "st_control": st_control,
                        "transaction_version": txn_version,
                        "claim_id": None, "claim_amount": None, "place_of_service": None,
                        "subscriber_id": subscriber_id, "patient_id": patient_id,
                        "billing_npi": billing_npi, "rendering_npi": None,
                        "claim_from_dt": None, "claim_to_dt": None,
                        "service_lines_json": None, "claim_block_text": None,
                        "parse_status": "FAIL", "error_stage": "no_clm",
                        "error_message": "Found ST*837 but no CLM segments"
                    })
                else:
                    for idx_pos, start_k in enumerate(clm_idx):
                        end_k = clm_idx[idx_pos+1] if idx_pos+1 < len(clm_idx) else len(txn_segs)
                        claim_block = txn_segs[start_k:end_k]

                        clm = claim_block[0].split(elem_sep)
                        claim_id = clm[1] if len(clm) > 1 else None
                        claim_amt = _safe_float(clm[2]) if len(clm) > 2 else None

                        # CLM05-1 place of service: CLM[5] like "11:B:1"
                        pos = None
                        if len(clm) > 5 and clm[5]:
                            pos = clm[5].split(":")[0] if ":" in clm[5] else clm[5]

                        # claim dates (basic): DTP*434 for statement, DTP*472 for service lines (we’ll capture first/last 472 too)
                        claim_from = None
                        claim_to = None
                        service_dates = []

                        rendering_npi = None
                        service_lines = []
                        current_lx = None

                        for s2 in claim_block:
                            # Rendering provider NM1*82...*XX*NPI
                            if s2.startswith("NM1"+elem_sep+"82"+elem_sep):
                                p2 = s2.split(elem_sep)
                                if "XX" in p2:
                                    rendering_npi = p2[-1]

                            # Dates
                            if s2.startswith("DTP"+elem_sep):
                                d = s2.split(elem_sep)
                                if len(d) >= 4:
                                    dtp01 = d[1]
                                    dt_val = d[3]
                                    if dtp01 == "434":  # statement date (often claim-level)
                                        if claim_from is None:
                                            claim_from = dt_val
                                    if dtp01 == "472":  # service date
                                        service_dates.append(dt_val)

                            # Service lines
                            if s2.startswith("LX"+elem_sep):
                                lx_parts = s2.split(elem_sep)
                                current_lx = lx_parts[1] if len(lx_parts) > 1 else None

                            if s2.startswith("SV1"+elem_sep):
                                sv = s2.split(elem_sep)
                                # SV1-1: "HC:99213[:mod...]"
                                qualifier = None
                                proc_code = None
                                modifiers = []
                                if len(sv) > 1 and sv[1]:
                                    comps = sv[1].split(":")
                                    qualifier = comps[0] if len(comps) > 0 else None
                                    proc_code = comps[1] if len(comps) > 1 else None
                                    modifiers = comps[2:] if len(comps) > 2 else []
                                charge = _safe_float(sv[2]) if len(sv) > 2 else None
                                units = _safe_float(sv[4]) if len(sv) > 4 else None

                                service_lines.append({
                                    "lx": current_lx,
                                    "qualifier": qualifier,
                                    "procedure_code": proc_code,
                                    "modifiers": modifiers,
                                    "charge_amount": charge,
                                    "units": units
                                })

                        if service_dates:
                            claim_from = claim_from or min(service_dates)
                            claim_to = max(service_dates)

                        rows.append({
                            "vendor_id": vendor_id,
                            "file_hash": file_hash,
                            "file_path": file_path,
                            "run_id": bronze_run_id,
                            "parse_ts": parse_ts,
                            "isa_control": isa_control,
                            "gs_control": gs_control,
                            "st_control": st_control,
                            "transaction_version": txn_version,
                            "claim_id": claim_id,
                            "claim_amount": claim_amt,
                            "place_of_service": pos,
                            "subscriber_id": subscriber_id,
                            "patient_id": patient_id,
                            "billing_npi": billing_npi,
                            "rendering_npi": rendering_npi,
                            "claim_from_dt": claim_from,
                            "claim_to_dt": claim_to,
                            "service_lines_json": json.dumps(service_lines),
                            # keep small debug snippet (don’t store huge blocks in prod)
                            "claim_block_text": (seg_sep.join(claim_block))[:4000],
                            "parse_status": "SUCCESS",
                            "error_stage": None,
                            "error_message": None
                        })

                i = j + 1
                continue
        i += 1

    if not found_837:
        return [{
            "vendor_id": vendor_id,
            "file_hash": file_hash,
            "file_path": file_path,
            "run_id": bronze_run_id,
            "parse_ts": parse_ts,
            "isa_control": isa_control,
            "gs_control": gs_control,
            "st_control": None,
            "transaction_version": None,
            "claim_id": None,
            "claim_amount": None,
            "place_of_service": None,
            "subscriber_id": subscriber_id,
            "patient_id": patient_id,
            "billing_npi": billing_npi,
            "rendering_npi": None,
            "claim_from_dt": None,
            "claim_to_dt": None,
            "service_lines_json": None,
            "claim_block_text": None,
            "parse_status": "FAIL",
            "error_stage": "no_st_837",
            "error_message": "No ST*837 transaction found in file"
        }]

    return rows

# -----------------------------
# 3) mapInPandas (file -> claim rows)
# -----------------------------
out_schema = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("file_hash", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("parse_ts", TimestampType(), True),
    StructField("isa_control", StringType(), True),
    StructField("gs_control", StringType(), True),
    StructField("st_control", StringType(), True),
    StructField("transaction_version", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("place_of_service", StringType(), True),
    StructField("subscriber_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("billing_npi", StringType(), True),
    StructField("rendering_npi", StringType(), True),
    StructField("claim_from_dt", StringType(), True),
    StructField("claim_to_dt", StringType(), True),
    StructField("service_lines_json", StringType(), True),
    StructField("claim_block_text", StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("error_stage", StringType(), True),
    StructField("error_message", StringType(), True),
])

def parse_files_map(pdf_iter):
    for pdf in pdf_iter:
        out_rows = []
        for r in pdf.itertuples(index=False):
            try:
                out_rows.extend(
                    parse_837_file(
                        r.vendor_id,
                        r.file_hash,
                        r.file_path,
                        r.run_id,
                        r.content_str
                    )
                )
            except Exception as e:
                sample = (r.content_str[:300] if isinstance(r.content_str, str) else None)
                out_rows.append({
                    "vendor_id": r.vendor_id,
                    "file_hash": r.file_hash,
                    "file_path": r.file_path,
                    "run_id": r.run_id,
                    "parse_ts": pd.Timestamp.utcnow(),
                    "isa_control": None,
                    "gs_control": None,
                    "st_control": None,
                    "transaction_version": None,
                    "claim_id": None,
                    "claim_amount": None,
                    "place_of_service": None,
                    "subscriber_id": None,
                    "patient_id": None,
                    "billing_npi": None,
                    "rendering_npi": None,
                    "claim_from_dt": None,
                    "claim_to_dt": None,
                    "service_lines_json": None,
                    "claim_block_text": None,
                    "parse_status": "FAIL",
                    "error_stage": "exception",
                    "error_message": str(e)[:1000]
                })
        yield pd.DataFrame(out_rows)

# repartition by vendor for parallelism
df_in = df_new.select("vendor_id","file_hash","file_path","run_id","content_str").repartition("vendor_id")

df_parsed = df_in.mapInPandas(parse_files_map, schema=out_schema)

# -----------------------------
# 4) Split SUCCESS vs FAIL
# -----------------------------
df_claims = df_parsed.filter(F.col("parse_status") == "SUCCESS") \
    .select(
        "vendor_id","file_hash","file_path","run_id","parse_ts",
        "isa_control","gs_control","st_control","transaction_version",
        "claim_id","claim_amount","place_of_service",
        "subscriber_id","patient_id","billing_npi","rendering_npi",
        "claim_from_dt","claim_to_dt","service_lines_json","claim_block_text"
    )

df_errors = df_parsed.filter(F.col("parse_status") == "FAIL") \
    .withColumn("sample_text", F.lit(None)) \
    .select(
        "vendor_id","file_hash","file_path","run_id","parse_ts",
        F.col("error_stage"),
        F.col("error_message"),
        # small sample from file for troubleshooting
        F.substring(F.lit(""), 1, 1).alias("sample_text")  # placeholder, filled below
    )

# Better sample_text: join back to bronze for a preview
df_errors = (
    df_errors.drop("sample_text")
    .join(df_bronze.select("file_hash","content_str"), on="file_hash", how="left")
    .withColumn("sample_text", F.substring(F.col("content_str"), 1, 300))
    .drop("content_str")
)

print("Claims parsed:", df_claims.count())
print("Errors:", df_errors.count())

display(df_claims.select("vendor_id","file_path","st_control","claim_id","claim_amount","billing_npi","rendering_npi"))
display(df_errors)

# -----------------------------
# 5) Write to Silver (MERGE for idempotency)
# -----------------------------
df_claims.createOrReplaceTempView("stg_claims")
spark.sql(f"""
MERGE INTO {SILVER_CLAIM_TBL} t
USING stg_claims s
ON t.file_hash = s.file_hash AND t.st_control = s.st_control AND t.claim_id = s.claim_id
WHEN NOT MATCHED THEN INSERT *
""")

df_errors.createOrReplaceTempView("stg_err")
spark.sql(f"""
MERGE INTO {SILVER_ERR_TBL} t
USING stg_err s
ON t.file_hash = s.file_hash AND t.error_stage = s.error_stage AND t.error_message = s.error_message
WHEN NOT MATCHED THEN INSERT *
""")
print("✅ Step 3 complete: Silver claims + errors updated.")