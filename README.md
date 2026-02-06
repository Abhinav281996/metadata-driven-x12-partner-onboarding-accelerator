# Metadata-Driven X12 Partner Onboarding Accelerator (Databricks)

A metadata-driven framework to onboard new healthcare partners faster by ingesting, parsing, normalizing, and auditing **X12 837 claims** in Databricks—without building a new stored procedure or a new  job per partner.

This repo contains the **DDL/DML**, notebook logic (Bronze → Silver → Gold → Ops), and Step 7 metadata (vendor profiles, mappings, standardization, quarantine) to make the pipeline configuration-driven.

---

## What problem this solves

Traditional onboarding for new partners often requires:
- A separate stored procedure / Talend job / custom process per partner
- Manual changes when partner format varies (delimiters, missing segments, etc.)
- Limited observability (hard to reconcile totals and prove completeness)
- Slow onboarding cycles

This accelerator reduces onboarding time by shifting “partner differences” into **metadata tables** rather than new code.

---

## Key project facts (must match environment)

- **Catalog:** `artha_serverless_usa`
- **Landing base path:**  
  `/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/`
- **Vendors (folders under landing base):** `vendorA`, `vendorB`, `vendorC`
- **Notebooks live under:**  
  `/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/`

---

## Target architecture (Medallion)

### High-level flow
+-------------------+ +--------------------+ +--------------------+ +---------------------+
| Landing (Volumes) | --> | Bronze (raw files) | --> | Silver (parsed) | --> | Gold (curated model)|
| vendorA/vendorB | | content_str + meta | | claim JSON objects | | header/lines/etc. |
+-------------------+ +--------------------+ +--------------------+ +---------------------+
|
v
+---------------------+
| Ops (audit + DQ) |
| reconciliation, DQ |
+---------------------+


### Layers
- **Landing:** partner files arrive in vendor folders
- **Bronze:** store raw file + metadata (traceability)
- **Silver:** parse X12 → 1 row per claim (plus nested JSON structures)
- **Gold:** normalized claim tables (header, service lines, diagnosis, provider, subscriber, patient, other payer)
- **Ops:** file audit, reconciliation, data quality results, quarantine control

---

## How the framework is metadata-driven

Instead of hardcoding partner differences into pipelines, the behavior is controlled by config tables:

### Orchestration metadata
- `cfg_x12.vendor_registry`  
  Controls which vendors are active, where to look for files, regex patterns, expected version, max file size rules.
- `cfg_x12.pipeline_steps`  
  Controls which notebooks run and in what order (BRONZE → SILVER → GOLD → OPS).
- `cfg_x12.vendor_state` / `cfg_x12.vendor_step_overrides`  
  Tracks progress and allows per-vendor overrides (optional).

### Parsing + standardization metadata (Step 7)
- `cfg_x12.vendor_837_profile`  
  Controls parsing profile per vendor (ISA-based delimiter detection, strictness, expected version).
- `cfg_x12.nm1_role_map`, `cfg_x12.hi_qualifier_map`, `cfg_x12.ref_qualifier_map`  
  Mapping tables to normalize vendor differences without code changes.
- `cfg_x12.silver_standardization_rules`  
  Config-driven standardization actions like TRIM/UPPER/DEFAULT/REGEX.

### Operational metadata
- `cfg_x12.dq_rules_837`  
  Data quality rules stored as SQL predicates (global + vendor-specific).
- `ops_x12.quarantine_files`  
  File-level quarantine to isolate bad files and allow safe reprocessing.

---

## Repo contents (what you will find here)

- **`x12_837_partner_onboarding_all_ddl_dml.sql`**  
  One consolidated SQL script containing object creation + seed data (Steps 1–7 + vendor onboarding).
- Notebooks for each step (Bronze/Silver/Gold/Ops + standardization)
- Sample X12 files (vendorA/vendorB 837P, vendorC 837I) for testing

---

## Prerequisites

- Databricks workspace with **Unity Catalog enabled**
- Permissions to:
  - create schemas and tables in `artha_serverless_usa`
  - access/write to Volumes under `/Volumes/artha_serverless_usa/...`
- A cluster or serverless compute that can run notebooks and write Delta tables

---

## Setup (one-time)

### 1) Create objects + seed metadata
Run:
- `x12_837_partner_onboarding_all_ddl_dml.sql`

This creates:
- schemas: `cfg_x12`, `bronze_x12`, `silver_x12`, `gold_claims`, `ops_x12`
- Bronze/Silver/Gold/Ops tables
- metadata tables + default seed rows (vendors, pipeline steps, profiles, DQ rules)

### 2) Ensure landing folders exist
Landing base:
`/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/`

Expected vendor folders:
- `vendorA/`
- `vendorB/`
- `vendorC/`

### 3) Import notebooks into Databricks
Workspace folder:
`/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/`

---

## Running the pipeline (daily)

1) Drop X12 files into vendor landing folder (e.g., `vendorA/`)
2) Run orchestrator notebook:
   - `10_orchestrator_x12_837_vendor_driven.py`
3) Orchestrator will execute steps based on `cfg_x12.pipeline_steps`

---

## Onboarding a new vendor (inbound)

### What you add/update (mostly metadata)

1) Create vendor landing folder:
/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/<newVendor>/


2) Insert vendor in:
- `cfg_x12.vendor_registry` (landing_path, file_name_regex, expected_version, sizing rules)
- `cfg_x12.vendor_837_profile` (ISA delimiter detection, strict_mode, transaction type)

3) Add vendor-specific DQ rules if needed:
- `cfg_x12.dq_rules_837`

4) (Optional) add vendor-specific mappings:
- `cfg_x12.nm1_role_map`, `cfg_x12.hi_qualifier_map`, `cfg_x12.ref_qualifier_map`

5) Drop a test file and run orchestrator

✅ No new stored procedure  
✅ No new Talend job  
✅ Minimal code change only if a truly new parsing pattern is needed

---

## Operational model (audit, reconciliation, DQ, quarantine)

- **File audit:** `ops_x12.file_audit` tracks file status end-to-end  
- **Reconciliation:** `ops_x12.recon_summary` compares Silver vs Gold counts and totals  
- **DQ results:** `ops_x12.dq_results` stores failed counts per rule  
- **Quarantine:** `ops_x12.quarantine_files` isolates broken files (e.g., ISA delimiter detection failure when strict)

Typical statuses:
- RECEIVED → BRONZE_LOADED → SILVER_PARSED → GOLD_BUILT → SUCCESS
- QUARANTINED (bad file isolated; pipeline continues for other vendors/files)

---

## Security & governance notes (Unity Catalog)

- Use Unity Catalog schemas (`cfg_x12`, `bronze_x12`, `silver_x12`, `gold_claims`, `ops_x12`)
- Use Volumes for controlled landing storage under UC
- Apply RBAC:
  - Restrict write access to `cfg_x12` tables to admins/lead engineers
  - Allow read access to Gold for analytics consumers
  - Lock down Ops tables for audit/compliance visibility

---

## Performance considerations

- Use file-level idempotency via `file_hash` in Ops + merge patterns in Gold
- Prefer ISA-based delimiter detection (fast, reliable)
- Partitioning strategy (when scaling):
  - Silver/Gold partition by `vendor_id` and/or `load_dt` (if added)
- Avoid repeated “collect” of large datasets; for POC it’s fine, for scale switch to join-based filters
- Consider Photon + optimized Delta settings in production

---

## Known limitations & next steps

### Step 8 (not done yet): Outbound delivery framework
- Partner-specific extracts (format, filters, naming, delivery target)
- Delivery publish (SFTP/ADLS/API), manifest/control files, outbound audit

### Step 9 (not done yet): Production hardening
- CI/CD (Databricks Asset Bundles or pipeline promotion)
- Monitoring/alerts (job failures, DQ thresholds, SLA breaches)
- Auto-quarantine on ERROR DQ rules
- Improved incremental processing + retries + backfills
- Stronger schema/version enforcement per X12 implementation guide

---

## Quick SQL snippets (common checks)

### Check vendor registry
```sql
SELECT vendor_id, landing_path, transaction_type, expected_version, active_flag
FROM artha_serverless_usa.cfg_x12.vendor_registry;
