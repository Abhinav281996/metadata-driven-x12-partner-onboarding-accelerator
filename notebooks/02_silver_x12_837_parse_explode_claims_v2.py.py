# Databricks notebook source
# DBTITLE 1,Cell 1
# 02_silver_x12_837_parse_explode_claims_v2.py
# Step 3 (extended): Bronze -> Silver (1 row per claim) + diagnosis/providers/subscriber/patient/other_payer JSON
#
# NOTE about reruns:
# - This version includes WHEN MATCHED UPDATE so existing rows get populated with new columns.
#
import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)


# -----------------------------
# 0) Orchestrator widgets (safe when run manually)
# -----------------------------
try:
    dbutils.widgets.text("vendor_id", "")
    dbutils.widgets.text("landing_path", "")
    dbutils.widgets.text("file_paths_json", "")
    dbutils.widgets.text("master_run_id", "")
    dbutils.widgets.text("triggered_by", "")
except Exception:
    pass

def _get_widget(name: str):
    try:
        v = dbutils.widgets.get(name)
        v = v.strip() if isinstance(v, str) else v
        return v or None
    except Exception:
        return None

vendor_id = _get_widget("vendor_id")
landing_path = _get_widget("landing_path")
file_paths_json = _get_widget("file_paths_json")
master_run_id = _get_widget("master_run_id")
triggered_by = _get_widget("triggered_by")


CATALOG = "artha_serverless_usa"
BRONZE_TBL = f"{CATALOG}.bronze_x12.bronze_837_raw"
SILVER_CLAIM_TBL = f"{CATALOG}.silver_x12.silver_837_claim"
SILVER_ERR_TBL = f"{CATALOG}.silver_x12.silver_837_parse_error"

run_id = spark.sql("select uuid() as id").collect()[0]["id"]
print("RUN_ID:", run_id)

# -----------------------------
# 1) Read only NEW/UNPROCESSED bronze files (based on file_hash presence)
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
# 2) Parser helpers (POC, extended)
# -----------------------------

# ---- Step 7: delimiter detection + vendor profile (mapInPandas-safe) ----
def detect_delimiters_from_isa(x12_text: str):
    """
    X12 ISA is fixed-length.
    element sep is ISA position 4 (0-based idx+3 after 'ISA')
    repetition sep is ISA11 (0-based idx+82)
    component sep is ISA16 (0-based idx+104)
    segment terminator is 1 char after ISA16 (0-based idx+105)
    """
    if not x12_text:
        return None
    idx = x12_text.find("ISA")
    if idx < 0 or len(x12_text) < idx + 106:
        return None

    element_delim = x12_text[idx + 3]
    repetition_delim = x12_text[idx + 82]
    component_delim = x12_text[idx + 104]
    segment_delim = x12_text[idx + 105]

    return {
        "segment_delim": segment_delim,
        "element_delim": element_delim,
        "component_delim": component_delim,
        "repetition_delim": repetition_delim
    }

def _default_profile():
    return {
        "use_isa_delimiters": True,
        "segment_delim": "~",
        "element_delim": "*",
        "component_delim": ":",
        "repetition_delim": "^",
        "strict_mode": True
    }

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

ROLE_MAP = {
    "85": "BILLING",
    "82": "RENDERING",
    "DN": "REFERRING",
    "77": "FACILITY",
    "72": "ATTENDING",
    "71": "OPERATING",
    "ZZ": "OTHER"
}

def _extract_nm1_info(nm1_seg, elem_sep):
    # NM1*<code>*<entity_type>*<last/org>*<first>*...*<id_qual>*<id>
    p = nm1_seg.split(elem_sep)
    code = p[1] if len(p) > 1 else None
    ent_type = p[2] if len(p) > 2 else None
    last_org = p[3] if len(p) > 3 else None
    first = p[4] if len(p) > 4 else None
    id_qual = p[8] if len(p) > 8 else None
    ident = p[9] if len(p) > 9 else (p[-1] if len(p) else None)
    role = ROLE_MAP.get(code, code)
    npi = ident if id_qual == "XX" else None
    return {
        "role": role,
        "nm1_code": code,
        "entity_type": ent_type,
        "last_name_or_org": last_org,
        "first_name": first,
        "id_qualifier": id_qual,
        "id_value": ident,
        "npi": npi
    }

def _extract_diagnoses_from_hi(hi_seg, elem_sep):
    # HI*ABK:E119*ABF:Z12345 ...
    parts = hi_seg.split(elem_sep)
    out = []
    for token in parts[1:]:
        if not token:
            continue
        comps = token.split(":")
        qual = comps[0] if len(comps) > 0 else None
        code = comps[1] if len(comps) > 1 else None
        if qual or code:
            out.append({"diag_qualifier": qual, "diag_code": code, "raw": token})
    return out

def parse_837_file(vendor_id, file_hash, file_path, bronze_run_id, content_str, profiles):
    """
    Returns a list of dict rows:
    - SUCCESS rows: one per claim, includes diagnosis_json/providers_json/subscriber_json/patient_json/other_payer_json
    - FAIL row: single row with error info
    """
    parse_ts = pd.Timestamp.utcnow()

    if content_str is None or not isinstance(content_str, str) or not content_str.startswith("ISA"):
        return [{
            "vendor_id": vendor_id, "file_hash": file_hash, "file_path": file_path,
            "run_id": bronze_run_id, "parse_ts": parse_ts,
            "isa_control": None, "gs_control": None, "st_control": None, "transaction_version": None,
            "claim_id": None, "claim_amount": None, "place_of_service": None,
            "subscriber_id": None, "patient_id": None, "billing_npi": None, "rendering_npi": None,
            "claim_from_dt": None, "claim_to_dt": None,
            "service_lines_json": None, "claim_block_text": None,
            "diagnosis_json": None, "providers_json": None,
            "subscriber_json": None, "patient_json": None, "other_payer_json": None,
            "parse_status": "FAIL", "error_stage": "basic_check",
            "error_message": "Content is null or does not start with ISA"
        }]

    # separators (Step 7): detect from ISA (preferred), else vendor profile fallbacks
    profile = profiles.get(vendor_id) or _default_profile()

    delims = detect_delimiters_from_isa(content_str) if profile.get("use_isa_delimiters", True) else None
    if not delims:
        delims = {
            "segment_delim": profile.get("segment_delim", "~"),
            "element_delim": profile.get("element_delim", "*"),
            "component_delim": profile.get("component_delim", ":"),
            "repetition_delim": profile.get("repetition_delim", "^"),
        }
        if profile.get("strict_mode", True):
            return [{
                "vendor_id": vendor_id, "file_hash": file_hash, "file_path": file_path,
                "run_id": bronze_run_id, "parse_ts": parse_ts,
                "isa_control": None, "gs_control": None, "st_control": None, "transaction_version": None,
                "claim_id": None, "claim_amount": None, "place_of_service": None,
                "subscriber_id": None, "patient_id": None, "billing_npi": None, "rendering_npi": None,
                "claim_from_dt": None, "claim_to_dt": None,
                "service_lines_json": None, "claim_block_text": None,
                "diagnosis_json": None, "providers_json": None,
                "subscriber_json": None, "patient_json": None, "other_payer_json": None,
                "parse_status": "FAIL",
                "error_stage": "quarantine_delimiters",
                "error_message": "ISA delimiter detection failed (strict_mode=true)"
            }]

    elem_sep = delims["element_delim"]
    seg_sep = delims["segment_delim"]

    segments = [s.strip() for s in content_str.split(seg_sep) if s and s.strip()]
    if not segments:
        return [{
            "vendor_id": vendor_id, "file_hash": file_hash, "file_path": file_path,
            "run_id": bronze_run_id, "parse_ts": parse_ts,
            "isa_control": None, "gs_control": None, "st_control": None, "transaction_version": None,
            "claim_id": None, "claim_amount": None, "place_of_service": None,
            "subscriber_id": None, "patient_id": None, "billing_npi": None, "rendering_npi": None,
            "claim_from_dt": None, "claim_to_dt": None,
            "service_lines_json": None, "claim_block_text": None,
            "diagnosis_json": None, "providers_json": None,
            "subscriber_json": None, "patient_json": None, "other_payer_json": None,
            "parse_status": "FAIL", "error_stage": "split_segments",
            "error_message": "No segments after splitting"
        }]

    # Envelope controls
    isa_control = None
    gs_control = None
    try:
        isa = next(s for s in segments if s.startswith("ISA"+elem_sep))
        isa_parts = isa.split(elem_sep)
        isa_control = isa_parts[13] if len(isa_parts) > 13 else None
    except Exception:
        pass
    try:
        gs = next(s for s in segments if s.startswith("GS"+elem_sep))
        gs_parts = gs.split(elem_sep)
        gs_control = gs_parts[6] if len(gs_parts) > 6 else None
    except Exception:
        pass

    # File-level parties (subscriber/patient/billing) - applies to claims in typical 837P flows
    billing_provider = {}
    subscriber = {}
    patient = {}

    current_party = None  # "SUB" or "PAT" or "BILL"
    for s in segments:
        if s.startswith("NM1"+elem_sep+"85"+elem_sep):
            current_party = "BILL"
            billing_provider = _extract_nm1_info(s, elem_sep)
        elif s.startswith("NM1"+elem_sep+"IL"+elem_sep):
            current_party = "SUB"
            info = _extract_nm1_info(s, elem_sep)
            subscriber.update({
                "member_id": info.get("id_value"),
                "last_name": info.get("last_name_or_org"),
                "first_name": info.get("first_name"),
            })
        elif s.startswith("NM1"+elem_sep+"QC"+elem_sep):
            current_party = "PAT"
            info = _extract_nm1_info(s, elem_sep)
            patient.update({
                "member_id": info.get("id_value"),
                "last_name": info.get("last_name_or_org"),
                "first_name": info.get("first_name"),
            })
        elif s.startswith("DMG"+elem_sep) and current_party in ("SUB","PAT"):
            p = s.split(elem_sep)
            # DMG*D8*YYYYMMDD*M/F
            dob = p[2] if len(p) > 2 else None
            gender = p[3] if len(p) > 3 else None
            if current_party == "SUB":
                subscriber.update({"dob": dob, "gender": gender})
            else:
                patient.update({"dob": dob, "gender": gender})
        elif s.startswith("N3"+elem_sep) and current_party in ("SUB","PAT","BILL"):
            p = s.split(elem_sep)
            addr1 = p[1] if len(p) > 1 else None
            if current_party == "SUB":
                subscriber.update({"addr1": addr1})
            elif current_party == "PAT":
                patient.update({"addr1": addr1})
            else:
                billing_provider.update({"addr1": addr1})
        elif s.startswith("N4"+elem_sep) and current_party in ("SUB","PAT","BILL"):
            p = s.split(elem_sep)
            city = p[1] if len(p) > 1 else None
            state = p[2] if len(p) > 2 else None
            zipc = p[3] if len(p) > 3 else None
            if current_party == "SUB":
                subscriber.update({"city": city, "state": state, "zip": zipc})
            elif current_party == "PAT":
                patient.update({"city": city, "state": state, "zip": zipc})
            else:
                billing_provider.update({"city": city, "state": state, "zip": zipc})

    billing_npi = billing_provider.get("npi")
    subscriber_id = subscriber.get("member_id")
    patient_id = patient.get("member_id") or subscriber_id
    if not patient:
        patient = dict(subscriber)

    subscriber_json = json.dumps(subscriber) if subscriber else None
    patient_json = json.dumps(patient) if patient else None

    # Parse each ST..SE 837 transaction, split claims by CLM
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

                # claim boundaries by CLM
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
                        "service_lines_json": None,
                        "claim_block_text": None,
                        "diagnosis_json": None,
                        "providers_json": None,
                        "subscriber_json": subscriber_json,
                        "patient_json": patient_json,
                        "other_payer_json": None,
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

                        pos = None
                        if len(clm) > 5 and clm[5]:
                            pos = clm[5].split(":")[0] if ":" in clm[5] else clm[5]

                        claim_from = None
                        claim_to = None
                        service_dates = []

                        rendering_npi = None
                        service_lines = []
                        current_lx = None

                        diagnoses = []
                        providers = []
                        other_payers = []

                        for s2 in claim_block:
                            # Providers inside claim block
                            if s2.startswith("NM1"+elem_sep):
                                pinfo = _extract_nm1_info(s2, elem_sep)
                                if pinfo.get("role") in ("RENDERING","REFERRING","FACILITY","ATTENDING","OPERATING"):
                                    providers.append(pinfo)
                                    if pinfo.get("role") == "RENDERING" and pinfo.get("npi"):
                                        rendering_npi = pinfo.get("npi")

                            # Diagnosis
                            if s2.startswith("HI"+elem_sep):
                                diagnoses.extend(_extract_diagnoses_from_hi(s2, elem_sep))

                            # Other payer (light POC): capture NM1*PR inside claim block
                            if s2.startswith("NM1"+elem_sep+"PR"+elem_sep):
                                p = s2.split(elem_sep)
                                payer_name = p[3] if len(p) > 3 else None
                                payer_id = p[9] if len(p) > 9 else (p[-1] if len(p) else None)
                                other_payers.append({"payer_name": payer_name, "payer_id": payer_id})

                            # Dates
                            if s2.startswith("DTP"+elem_sep):
                                d = s2.split(elem_sep)
                                if len(d) >= 4:
                                    dtp01 = d[1]
                                    dt_val = d[3]
                                    if dtp01 == "434" and claim_from is None:
                                        claim_from = dt_val
                                    if dtp01 == "472":
                                        service_dates.append(dt_val)

                            # Service lines
                            if s2.startswith("LX"+elem_sep):
                                lx_parts = s2.split(elem_sep)
                                current_lx = lx_parts[1] if len(lx_parts) > 1 else None

                            if s2.startswith("SV1"+elem_sep):
                                sv = s2.split(elem_sep)
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

                            if s2.startswith("SV2"+elem_sep):
                                sv = s2.split(elem_sep)
                                revenue_code = None
                                qualifier = None
                                proc_code = None
                                modifiers = []
                                if len(sv) > 1 and sv[1]:
                                    comps = sv[1].split(":")
                                    revenue_code = comps[0] if len(comps) > 0 else None
                                    qualifier = comps[1] if len(comps) > 1 else None
                                    proc_code = comps[2] if len(comps) > 2 else None
                                    modifiers = comps[3:] if len(comps) > 3 else []
                                charge = _safe_float(sv[2]) if len(sv) > 2 else None
                                units = _safe_float(sv[4]) if len(sv) > 4 else None

                                service_lines.append({
                                    "lx": current_lx,
                                    "revenue_code": revenue_code,
                                    "qualifier": qualifier,
                                    "procedure_code": proc_code,
                                    "modifiers": modifiers,
                                    "charge_amount": charge,
                                    "units": units
                                })

                        # add billing provider (file-level)
                        if billing_provider:
                            bp = dict(billing_provider)
                            bp["role"] = "BILLING"
                            providers = [bp] + providers

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
                            "claim_block_text": (seg_sep.join(claim_block))[:4000],
                            "diagnosis_json": json.dumps(diagnoses) if diagnoses else None,
                            "providers_json": json.dumps(providers) if providers else None,
                            "subscriber_json": subscriber_json,
                            "patient_json": patient_json,
                            "other_payer_json": json.dumps(other_payers) if other_payers else None,
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
            "diagnosis_json": None,
            "providers_json": None,
            "subscriber_json": subscriber_json,
            "patient_json": patient_json,
            "other_payer_json": None,
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

    StructField("diagnosis_json", StringType(), True),
    StructField("providers_json", StringType(), True),
    StructField("subscriber_json", StringType(), True),
    StructField("patient_json", StringType(), True),
    StructField("other_payer_json", StringType(), True),

    StructField("parse_status", StringType(), True),
    StructField("error_stage", StringType(), True),
    StructField("error_message", StringType(), True),
])

# -----------------------------
# 3A) Load vendor parsing profiles for Step 7 delimiter handling
# -----------------------------
VENDOR_PROFILE_TBL = f"{CATALOG}.cfg_x12.vendor_837_profile"
profiles = {}
try:
    rows = (
        spark.table(VENDOR_PROFILE_TBL)
        .select(
            "vendor_id","use_isa_delimiters","segment_delim","element_delim",
            "component_delim","repetition_delim","strict_mode","updated_ts"
        )
        .orderBy(F.col("updated_ts").desc())
        .collect()
    )
    # latest row wins per vendor_id
    for r in rows:
        vid = r["vendor_id"]
        if vid and vid not in profiles:
            profiles[vid] = {
                "use_isa_delimiters": bool(r["use_isa_delimiters"]) if r["use_isa_delimiters"] is not None else True,
                "segment_delim": r["segment_delim"] or "~",
                "element_delim": r["element_delim"] or "*",
                "component_delim": r["component_delim"] or ":",
                "repetition_delim": r["repetition_delim"] or "^",
                "strict_mode": bool(r["strict_mode"]) if r["strict_mode"] is not None else True
            }
except Exception as e:
    print("WARN: could not load cfg_x12.vendor_837_profile; using defaults. Error:", str(e)[:300])

def parse_files_map(pdf_iter):
    for pdf in pdf_iter:
        out_rows = []
        for r in pdf.itertuples(index=False):
            try:
                out_rows.extend(parse_837_file(
                    r.vendor_id, r.file_hash, r.file_path, r.run_id, r.content_str, profiles
                ))
            except Exception as e:
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
                    "diagnosis_json": None,
                    "providers_json": None,
                    "subscriber_json": None,
                    "patient_json": None,
                    "other_payer_json": None,
                    "parse_status": "FAIL",
                    "error_stage": "exception",
                    "error_message": str(e)[:1000]
                })
        yield pd.DataFrame(out_rows)


df_in = df_new.select("vendor_id","file_hash","file_path","run_id","content_str").repartition("vendor_id")
df_parsed = df_in.mapInPandas(parse_files_map, schema=out_schema)

# -----------------------------
# 4) Split SUCCESS vs FAIL
# -----------------------------
df_claims = df_parsed.filter(F.col("parse_status") == "SUCCESS").select(
    "vendor_id","file_hash","file_path","run_id","parse_ts",
    "isa_control","gs_control","st_control","transaction_version",
    "claim_id","claim_amount","place_of_service",
    "subscriber_id","patient_id","billing_npi","rendering_npi",
    "claim_from_dt","claim_to_dt",
    "service_lines_json","claim_block_text",
    "diagnosis_json","providers_json","subscriber_json","patient_json","other_payer_json"
)

df_errors = (
    df_parsed.filter(F.col("parse_status") == "FAIL")
    .join(df_bronze.select("file_hash","content_str"), on="file_hash", how="left")
    .withColumn("sample_text", F.substring(F.col("content_str"), 1, 300))
    .drop("content_str")
    .select(
        "vendor_id","file_hash","file_path","run_id","parse_ts",
        F.col("error_stage"),
        F.col("error_message"),
        "sample_text"
    )
)


# -----------------------------
# 4A) Step 7 Quarantine writes (optional): persist delimiter failures for orchestrator skip
# -----------------------------
QUAR_TBL = f"{CATALOG}.ops_x12.quarantine_files"

try:
    df_quar = (
        df_errors
        .filter(F.col("error_stage") == "quarantine_delimiters")
        .select(
            "vendor_id","file_hash","file_path","run_id",
            F.lit("SILVER").alias("stage"),
            F.lit("SISA").alias("rule_id"),
            F.col("error_message").alias("reason"),
            F.current_timestamp().alias("created_ts")
        )
        .dropDuplicates(["vendor_id","file_hash","file_path","stage","rule_id"])
    )

    if df_quar.count() > 0:
        df_quar.createOrReplaceTempView("stg_quarantine")
        spark.sql(f"""
          MERGE INTO {QUAR_TBL} t
          USING stg_quarantine s
          ON t.vendor_id = s.vendor_id AND t.file_hash = s.file_hash AND t.stage = s.stage AND t.rule_id = s.rule_id
          WHEN MATCHED THEN UPDATE SET
            t.file_path = s.file_path,
            t.run_id = s.run_id,
            t.reason = s.reason,
            t.created_ts = s.created_ts
          WHEN NOT MATCHED THEN INSERT *
        """)
except Exception as e:
    print("WARN: quarantine write skipped (table may not exist yet). Error:", str(e)[:300])


print("Claims parsed:", df_claims.count())
print("Errors:", df_errors.count())

# -----------------------------
# 5) Write to Silver (MERGE with UPDATE to backfill new columns)
# -----------------------------
df_claims.createOrReplaceTempView("stg_claims")

spark.sql(f"""
MERGE INTO {SILVER_CLAIM_TBL} t
USING stg_claims s
ON t.file_hash = s.file_hash AND t.st_control <=> s.st_control AND t.claim_id <=> s.claim_id
WHEN MATCHED THEN UPDATE SET
  t.vendor_id = s.vendor_id,
  t.file_path = s.file_path,
  t.run_id = s.run_id,
  t.parse_ts = s.parse_ts,
  t.isa_control = s.isa_control,
  t.gs_control = s.gs_control,
  t.transaction_version = s.transaction_version,
  t.claim_amount = s.claim_amount,
  t.place_of_service = s.place_of_service,
  t.subscriber_id = s.subscriber_id,
  t.patient_id = s.patient_id,
  t.billing_npi = s.billing_npi,
  t.rendering_npi = s.rendering_npi,
  t.claim_from_dt = s.claim_from_dt,
  t.claim_to_dt = s.claim_to_dt,
  t.service_lines_json = s.service_lines_json,
  t.claim_block_text = s.claim_block_text,
  t.diagnosis_json = s.diagnosis_json,
  t.providers_json = s.providers_json,
  t.subscriber_json = s.subscriber_json,
  t.patient_json = s.patient_json,
  t.other_payer_json = s.other_payer_json
WHEN NOT MATCHED THEN INSERT *
""")

df_errors.createOrReplaceTempView("stg_err")
spark.sql(f"""
MERGE INTO {SILVER_ERR_TBL} t
USING stg_err s
ON t.file_hash = s.file_hash AND t.error_stage = s.error_stage AND t.error_message = s.error_message
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Step 3 (extended) complete: Silver claims + errors updated.")