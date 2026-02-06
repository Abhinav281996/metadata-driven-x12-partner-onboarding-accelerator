-- ======================================================================================
-- X12 837 Metadata-Driven Partner Onboarding Framework (Databricks)
-- Consolidated DDL + DML (Steps 1–7 + VendorC onboarding)
--
-- Catalog: artha_serverless_usa
-- Landing base: /Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/
-- Vendors: vendorA, vendorB, vendorC (837I)
-- Notebook base (for pipeline_steps): /Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/
--
-- Notes
-- - This script is written to be re-runnable (idempotent) using CREATE IF NOT EXISTS + MERGE.
-- - Delta tables do not enforce primary keys; idempotency is achieved via MERGE keys (run/file hashes).
-- ======================================================================================

-- ---------------------------------------
-- 0) Set catalog + create schemas
-- -----------------------------------------
USE CATALOG artha_serverless_usa;

CREATE SCHEMA IF NOT EXISTS cfg_x12;
CREATE SCHEMA IF NOT EXISTS bronze_x12;
CREATE SCHEMA IF NOT EXISTS silver_x12;
CREATE SCHEMA IF NOT EXISTS gold_claims;
CREATE SCHEMA IF NOT EXISTS ops_x12;

-- ======================================================================================
-- STEP 1: Bronze (raw ingest)
-- ======================================================================================
CREATE TABLE IF NOT EXISTS bronze_x12.bronze_837_raw (
  vendor_id        STRING,
  file_hash        STRING,
  file_path        STRING,
  file_name        STRING,
  file_size_bytes  BIGINT,
  ingestion_ts     TIMESTAMP,
  bronze_run_id    STRING,
  triggered_by     STRING,
  content_str      STRING
)
USING DELTA;

-- ======================================================================================
-- STEP 2/3: Silver (parse + explode)
-- ======================================================================================
CREATE TABLE IF NOT EXISTS silver_x12.silver_837_claim (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,

  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,

  claim_id            STRING,
  claim_amount        DOUBLE,
  place_of_service    STRING,
  subscriber_id       STRING,
  patient_id          STRING,
  billing_npi         STRING,
  rendering_npi       STRING,
  claim_from_dt       DATE,
  claim_to_dt         DATE,

  -- nested structures stored as JSON for flexibility
  service_lines_json  STRING,
  diagnosis_json      STRING,
  providers_json      STRING,
  subscriber_json     STRING,
  patient_json        STRING,
  other_payer_json    STRING,

  -- for traceability/debug (kept in POC; can be removed later)
  claim_block_text    STRING,

  -- parser status
  parse_status        STRING,     -- SUCCESS/FAIL
  error_stage         STRING,
  error_message       STRING
)
USING DELTA;

-- ======================================================================================
-- STEP 4: Gold (curated claim model)
-- ======================================================================================

-- 4.0 Claim header (one row per claim)
CREATE TABLE IF NOT EXISTS gold_claims.claim_header (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,

  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,

  claim_id            STRING,
  claim_key           STRING,      -- hashed business key for idempotent merges
  claim_amount        DOUBLE,
  place_of_service    STRING,
  claim_from_dt       DATE,
  claim_to_dt         DATE,

  subscriber_id       STRING,
  patient_id          STRING,
  billing_npi         STRING,
  rendering_npi       STRING
)
USING DELTA;

-- 4.0 Claim service line (one row per line)
-- Supports both 837P (SV1) and 837I (SV2) by keeping generic columns.
CREATE TABLE IF NOT EXISTS gold_claims.claim_service_line (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,

  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,

  claim_id            STRING,
  claim_key           STRING,

  line_seq            INT,
  procedure_code      STRING,     -- HCPCS/CPT (often from SV1/SV2)
  rev_code            STRING,     -- 837I institutional revenue code (if available)
  modifier1           STRING,
  modifier2           STRING,
  modifier3           STRING,
  modifier4           STRING,
  units               DOUBLE,
  line_charge_amount  DOUBLE,
  line_from_dt        DATE,
  line_to_dt          DATE,

  service_line_key    STRING
)
USING DELTA;

-- 4.1 Additional gold tables (from extended DDL)
-- (diagnosis, providers, subscriber/patient, other payer)
CREATE TABLE IF NOT EXISTS gold_claims.claim_diagnosis (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,
  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,
  claim_id            STRING,
  claim_key           STRING,
  diag_seq            INT,
  diag_qualifier      STRING,
  diag_code           STRING,
  diag_raw            STRING,
  diagnosis_key       STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_claims.claim_provider (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,
  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,
  claim_id            STRING,
  claim_key           STRING,
  provider_role       STRING,
  nm1_code            STRING,
  entity_type         STRING,
  last_name_or_org    STRING,
  first_name          STRING,
  id_qualifier        STRING,
  id_value            STRING,
  npi                 STRING,
  addr1               STRING,
  city                STRING,
  state               STRING,
  zip                 STRING,
  provider_key        STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_claims.claim_subscriber (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,
  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,
  claim_id            STRING,
  claim_key           STRING,
  member_id           STRING,
  first_name          STRING,
  last_name           STRING,
  dob                 DATE,
  gender              STRING,
  addr1               STRING,
  city                STRING,
  state               STRING,
  zip                 STRING,
  subscriber_key      STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_claims.claim_patient (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,
  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,
  claim_id            STRING,
  claim_key           STRING,
  member_id           STRING,
  first_name          STRING,
  last_name           STRING,
  dob                 DATE,
  gender              STRING,
  addr1               STRING,
  city                STRING,
  state               STRING,
  zip                 STRING,
  patient_key         STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_claims.claim_other_payer (
  vendor_id           STRING,
  file_hash           STRING,
  file_path           STRING,
  run_id              STRING,
  parse_ts            TIMESTAMP,
  load_ts             TIMESTAMP,
  isa_control         STRING,
  gs_control          STRING,
  st_control          STRING,
  transaction_version STRING,
  claim_id            STRING,
  claim_key           STRING,
  payer_seq           INT,
  payer_name          STRING,
  payer_id            STRING,
  other_payer_key     STRING
)
USING DELTA;

-- ======================================================================================
-- STEP 5: Ops/Audit + DQ framework
-- ======================================================================================

CREATE TABLE IF NOT EXISTS ops_x12.pipeline_run (
  pipeline_name     STRING,
  run_id            STRING,
  triggered_by      STRING,
  start_ts          TIMESTAMP,
  end_ts            TIMESTAMP,
  status            STRING,      -- RUNNING/SUCCESS/FAILED
  notes             STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops_x12.file_audit (
  vendor_id         STRING,
  file_hash         STRING,
  file_path         STRING,
  first_seen_ts     TIMESTAMP,
  last_updated_ts   TIMESTAMP,
  file_size_bytes   BIGINT,

  bronze_run_id     STRING,
  silver_run_id     STRING,
  gold_run_id       STRING,

  status            STRING,      -- RECEIVED/BRONZE_LOADED/SILVER_PARSED/GOLD_BUILT/FAILED/QUARANTINED
  error_stage       STRING,
  error_message     STRING,

  silver_claim_count        BIGINT,
  silver_service_line_count BIGINT,
  silver_total_claim_amount DOUBLE,

  gold_header_count         BIGINT,
  gold_service_line_count   BIGINT,
  gold_dx_count             BIGINT,
  gold_provider_count       BIGINT,
  gold_subscriber_count     BIGINT,
  gold_patient_count        BIGINT,
  gold_other_payer_count    BIGINT,

  recon_header_match        BOOLEAN,
  recon_service_line_match  BOOLEAN,
  recon_amount_match        BOOLEAN,
  recon_notes               STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops_x12.recon_summary (
  vendor_id            STRING,
  file_hash            STRING,
  st_control           STRING,
  transaction_version  STRING,

  expected_claims      BIGINT,
  parsed_claims        BIGINT,

  gold_headers         BIGINT,
  gold_lines           BIGINT,

  parsed_claim_amt     DOUBLE,
  header_claim_amt     DOUBLE,

  header_match         BOOLEAN,
  lines_match          BOOLEAN,
  amount_match         BOOLEAN,

  load_ts              TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.dq_rules_837 (
  rule_id           STRING,
  rule_name         STRING,
  entity            STRING,      -- SILVER_CLAIM / GOLD_HEADER / GOLD_SERVICE_LINE / etc
  severity          STRING,      -- INFO/WARN/ERROR
  vendor_id         STRING,      -- nullable
  active_flag       BOOLEAN,
  sql_predicate     STRING,      -- rows that FAIL the rule (SQL WHERE clause predicate)
  created_ts        TIMESTAMP,
  updated_ts        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops_x12.dq_results (
  pipeline_name     STRING,
  run_id            STRING,
  vendor_id         STRING,
  file_hash         STRING,
  rule_id           STRING,
  entity            STRING,
  severity          STRING,
  failed_count      BIGINT,
  sample_values     STRING,
  load_ts           TIMESTAMP
)
USING DELTA;

-- Seed global DQ rules (idempotent)
MERGE INTO cfg_x12.dq_rules_837 t
USING (
  SELECT 'R001' AS rule_id,'Silver claim must have claim_id' AS rule_name,'SILVER_CLAIM' AS entity,'ERROR' AS severity,CAST(NULL AS STRING) AS vendor_id,true AS active_flag,'claim_id IS NULL' AS sql_predicate,current_timestamp() AS created_ts,current_timestamp() AS updated_ts
  UNION ALL SELECT 'R002','Silver claim amount cannot be negative','SILVER_CLAIM','ERROR',NULL,true,'claim_amount < 0',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'R003','Gold header must have claim_id','GOLD_HEADER','ERROR',NULL,true,'claim_id IS NULL',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'R004','Gold service line must have procedure_code','GOLD_SERVICE_LINE','WARN',NULL,true,'procedure_code IS NULL',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'R005','Gold provider should have role','GOLD_PROVIDER','WARN',NULL,true,'provider_role IS NULL',current_timestamp(),current_timestamp()
) s
ON t.rule_id = s.rule_id AND (t.vendor_id <=> s.vendor_id)
WHEN MATCHED THEN UPDATE SET
  t.rule_name = s.rule_name,
  t.entity = s.entity,
  t.severity = s.severity,
  t.active_flag = s.active_flag,
  t.sql_predicate = s.sql_predicate,
  t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- ======================================================================================
-- STEP 6: Metadata tables (vendor registry + pipeline steps)
-- ======================================================================================

CREATE TABLE IF NOT EXISTS cfg_x12.vendor_registry (
  vendor_id              STRING,
  vendor_name            STRING,
  active_flag            BOOLEAN,
  transaction_type       STRING,
  landing_path           STRING,
  file_name_regex        STRING,
  expected_version       STRING,
  max_file_size_mb       INT,
  quarantine_on_oversize BOOLEAN,
  created_ts             TIMESTAMP,
  updated_ts             TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.vendor_state (
  vendor_id        STRING,
  pipeline_name    STRING,
  last_success_ts  TIMESTAMP,
  last_run_id      STRING,
  updated_ts       TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.pipeline_steps (
  pipeline_name        STRING,
  step_order           INT,
  step_name            STRING,
  notebook_path        STRING,
  timeout_seconds      INT,
  enabled_flag         BOOLEAN,
  default_params_json  STRING,
  created_ts           TIMESTAMP,
  updated_ts           TIMESTAMP
)
USING DELTA;

-- Optional overrides per vendor/step (for future)
CREATE TABLE IF NOT EXISTS cfg_x12.vendor_step_overrides (
  vendor_id            STRING,
  pipeline_name        STRING,
  step_name            STRING,
  enabled_override     BOOLEAN,
  params_override_json STRING,
  created_ts           TIMESTAMP,
  updated_ts           TIMESTAMP
)
USING DELTA;

-- Seed vendor registry for vendorA + vendorB (837P)
MERGE INTO cfg_x12.vendor_registry t
USING (
  SELECT
    'vendorA' AS vendor_id,
    'Vendor A (837P Professional)' AS vendor_name,
    true AS active_flag,
    '837P' AS transaction_type,
    '/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/vendorA' AS landing_path,
    '.*\.x12$' AS file_name_regex,
    '005010X222A1' AS expected_version,
    200 AS max_file_size_mb,
    true AS quarantine_on_oversize,
    current_timestamp() AS created_ts,
    current_timestamp() AS updated_ts
  UNION ALL
  SELECT
    'vendorB','Vendor B (837P Professional)',true,'837P',
    '/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/vendorB',
    '.*\.x12$','005010X222A1',200,true,current_timestamp(),current_timestamp()
  UNION ALL
  -- VendorC onboarding (837I Institutional)
  SELECT
    'vendorC','Vendor C (837I Institutional)',true,'837I',
    '/Volumes/artha_serverless_usa/x12/x12_landing_vol/landing/x12/837/vendorC',
    '.*\.x12$','005010X223A2',200,true,current_timestamp(),current_timestamp()
) s
ON t.vendor_id = s.vendor_id
WHEN MATCHED THEN UPDATE SET
  t.vendor_name = s.vendor_name,
  t.active_flag = s.active_flag,
  t.transaction_type = s.transaction_type,
  t.landing_path = s.landing_path,
  t.file_name_regex = s.file_name_regex,
  t.expected_version = s.expected_version,
  t.max_file_size_mb = s.max_file_size_mb,
  t.quarantine_on_oversize = s.quarantine_on_oversize,
  t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- Seed pipeline steps (order and notebook paths)
-- Adjust if you rename notebooks; these match your current folder structure.
MERGE INTO cfg_x12.pipeline_steps t
USING (
  SELECT
    'x12_837_onboarding' AS pipeline_name, 10 AS step_order, 'BRONZE' AS step_name,
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/01_bronze_x12_837_ingest_files' AS notebook_path,
    3600 AS timeout_seconds, true AS enabled_flag, '{}' AS default_params_json,
    current_timestamp() AS created_ts, current_timestamp() AS updated_ts
  UNION ALL SELECT 'x12_837_onboarding',20,'SILVER',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/02_silver_x12_837_parse_explode_claims_v2.py',
    7200,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',25,'SILVER_STD',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/11_silver_x12_apply_standardization_rules.py',
    3600,false,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',30,'GOLD_HEADER',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/03_gold_claims_837_normalize_header.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',40,'GOLD_LINES',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/04_gold_claims_837_normalize_service_lines.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',50,'GOLD_DX',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/05_gold_claims_837_normalize_diagnosis.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',60,'GOLD_PROVIDER',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/06_gold_claims_837_normalize_providers.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',70,'GOLD_SUB_PAT',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/07_gold_claims_837_normalize_subscriber_patient.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',80,'GOLD_OTHER_PAYER',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/08_gold_claims_837_normalize_other_payer.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'x12_837_onboarding',90,'OPS_AUDIT',
    '/Workspace/Users/abhinav.gaddam@thinkartha.com/HC_MetaData_Framework_PartnerOnBoarding/09_ops_x12_audit_reconciliation_and_dq.py',
    3600,true,'{}',current_timestamp(),current_timestamp()
) s
ON t.pipeline_name = s.pipeline_name AND t.step_order = s.step_order
WHEN MATCHED THEN UPDATE SET
  t.step_name = s.step_name,
  t.notebook_path = s.notebook_path,
  t.timeout_seconds = s.timeout_seconds,
  t.enabled_flag = s.enabled_flag,
  t.default_params_json = s.default_params_json,
  t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- ======================================================================================
-- STEP 7: Vendor profile + mappings + standardization + quarantine
-- ======================================================================================

CREATE TABLE IF NOT EXISTS cfg_x12.vendor_837_profile (
  vendor_id              STRING,
  transaction_type       STRING,   -- 837P / 837I
  use_isa_delimiters     BOOLEAN,

  segment_delim          STRING,
  element_delim          STRING,
  component_delim        STRING,
  repetition_delim       STRING,

  strict_mode            BOOLEAN,
  encoding               STRING,
  newline_normalization  STRING,
  expected_version       STRING,
  created_ts             TIMESTAMP,
  updated_ts             TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.nm1_role_map (
  vendor_id         STRING,    -- nullable for default (global)
  loop_id           STRING,
  nm101_entity_id   STRING,    -- NM101 (e.g., 85, 82, DN, 77)
  role_name         STRING,    -- BILLING / RENDERING / REFERRING / FACILITY / etc
  precedence        INT,
  active_flag       BOOLEAN,
  created_ts        TIMESTAMP,
  updated_ts        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.hi_qualifier_map (
  vendor_id         STRING,    -- nullable for default (global)
  hi_qualifier      STRING,
  diag_type         STRING,
  active_flag       BOOLEAN,
  created_ts        TIMESTAMP,
  updated_ts        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.ref_qualifier_map (
  vendor_id         STRING,    -- nullable for default (global)
  ref_qualifier     STRING,
  ref_type          STRING,
  active_flag       BOOLEAN,
  created_ts        TIMESTAMP,
  updated_ts        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS cfg_x12.silver_standardization_rules (
  rule_id            STRING,
  rule_name          STRING,
  vendor_id          STRING,      -- nullable = global
  target_table       STRING,      -- fully qualified table name
  target_column      STRING,
  action             STRING,      -- TRIM/UPPER/LOWER/REGEX_REPLACE/DEFAULT_IF_NULL
  action_params_json STRING,
  active_flag        BOOLEAN,
  created_ts         TIMESTAMP,
  updated_ts         TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops_x12.quarantine_files (
  vendor_id       STRING,
  file_hash       STRING,
  file_path       STRING,
  run_id          STRING,
  stage           STRING,      -- BRONZE/SILVER/GOLD/OPS
  rule_id         STRING,
  reason          STRING,
  created_ts      TIMESTAMP
)
USING DELTA;

-- Seed vendor parsing profiles (A/B = 837P; C = 837I)
MERGE INTO cfg_x12.vendor_837_profile t
USING (
  SELECT 'vendorA' AS vendor_id,'837P' AS transaction_type,true AS use_isa_delimiters,'~' AS segment_delim,'*' AS element_delim,':' AS component_delim,'^' AS repetition_delim,true AS strict_mode,'UTF-8' AS encoding,'NONE' AS newline_normalization,'005010X222A1' AS expected_version,current_timestamp() AS created_ts,current_timestamp() AS updated_ts
  UNION ALL
  SELECT 'vendorB','837P',true,'~','*',':','^',true,'UTF-8','NONE','005010X222A1',current_timestamp(),current_timestamp()
  UNION ALL
  SELECT 'vendorC','837I',true,'~','*',':','^',true,'UTF-8','NONE','005010X223A2',current_timestamp(),current_timestamp()
) s
ON t.vendor_id = s.vendor_id
WHEN MATCHED THEN UPDATE SET
  t.transaction_type = s.transaction_type,
  t.use_isa_delimiters = s.use_isa_delimiters,
  t.segment_delim = s.segment_delim,
  t.element_delim = s.element_delim,
  t.component_delim = s.component_delim,
  t.repetition_delim = s.repetition_delim,
  t.strict_mode = s.strict_mode,
  t.encoding = s.encoding,
  t.newline_normalization = s.newline_normalization,
  t.expected_version = s.expected_version,
  t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- Seed default mappings (global defaults): delete + insert to avoid duplicates
DELETE FROM cfg_x12.nm1_role_map WHERE vendor_id IS NULL;
DELETE FROM cfg_x12.ref_qualifier_map WHERE vendor_id IS NULL;
DELETE FROM cfg_x12.hi_qualifier_map WHERE vendor_id IS NULL;

INSERT INTO cfg_x12.nm1_role_map VALUES
(NULL, NULL, '85', 'BILLING', 10, true, current_timestamp(), current_timestamp()),
(NULL, NULL, '82', 'RENDERING', 10, true, current_timestamp(), current_timestamp()),
(NULL, NULL, '77', 'SERVICE_FACILITY', 10, true, current_timestamp(), current_timestamp()),
(NULL, NULL, 'DN', 'REFERRING', 10, true, current_timestamp(), current_timestamp());

INSERT INTO cfg_x12.ref_qualifier_map VALUES
(NULL, 'F8', 'ORIGINAL_REFERENCE', true, current_timestamp(), current_timestamp()),
(NULL, '1K', 'PAYER_CLAIM_CONTROL', true, current_timestamp(), current_timestamp()),
(NULL, 'EI', 'EMPLOYER_ID', true, current_timestamp(), current_timestamp());

INSERT INTO cfg_x12.hi_qualifier_map VALUES
(NULL, 'ABK', 'PRINCIPAL', true, current_timestamp(), current_timestamp()),
(NULL, 'ABF', 'ADMITTING', true, current_timestamp(), current_timestamp()),
(NULL, 'APR', 'OTHER', true, current_timestamp(), current_timestamp());

-- Seed basic Silver standardization rules (global defaults): delete + insert to avoid duplicates
DELETE FROM cfg_x12.silver_standardization_rules WHERE vendor_id IS NULL;

INSERT INTO cfg_x12.silver_standardization_rules VALUES
('S001','Trim claim_id',NULL,'artha_serverless_usa.silver_x12.silver_837_claim','claim_id','TRIM','{}',true,current_timestamp(),current_timestamp()),
('S002','Uppercase transaction_version',NULL,'artha_serverless_usa.silver_x12.silver_837_claim','transaction_version','UPPER','{}',true,current_timestamp(),current_timestamp()),
('S003','Default claim_amount to 0 if null',NULL,'artha_serverless_usa.silver_x12.silver_837_claim','claim_amount','DEFAULT_IF_NULL','{\"default\":0}',true,current_timestamp(),current_timestamp());

-- VendorC additional DQ rules (idempotent) - uses cfg_x12.dq_rules_837
MERGE INTO cfg_x12.dq_rules_837 t
USING (
  SELECT 'VC01' AS rule_id,'vendorC: subscriber_id is required' AS rule_name,'SILVER_CLAIM' AS entity,'ERROR' AS severity,'vendorC' AS vendor_id,true AS active_flag,'subscriber_id IS NULL' AS sql_predicate,current_timestamp() AS created_ts,current_timestamp() AS updated_ts
  UNION ALL SELECT 'VC02','vendorC: claim_amount must be > 0','SILVER_CLAIM','ERROR','vendorC',true,'claim_amount IS NULL OR claim_amount <= 0',current_timestamp(),current_timestamp()
  UNION ALL SELECT 'VC03','vendorC: at least one service line required','SILVER_CLAIM','ERROR','vendorC',true,"service_lines_json IS NULL OR service_lines_json = '[]'",current_timestamp(),current_timestamp()
  UNION ALL SELECT 'VC04','vendorC: diagnosis should exist','SILVER_CLAIM','WARN','vendorC',true,"diagnosis_json IS NULL OR diagnosis_json = '[]'",current_timestamp(),current_timestamp()
) s
ON t.rule_id = s.rule_id AND t.vendor_id = s.vendor_id
WHEN MATCHED THEN UPDATE SET
  t.rule_name = s.rule_name,
  t.entity = s.entity,
  t.severity = s.severity,
  t.active_flag = s.active_flag,
  t.sql_predicate = s.sql_predicate,
  t.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- ======================================================================================
-- END OF SCRIPT
-- ======================================================================================
