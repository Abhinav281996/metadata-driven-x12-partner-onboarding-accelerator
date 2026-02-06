# Metadata-Driven X12 Partner Onboarding Accelerator (Databricks)

A metadata-driven framework to onboard new healthcare partners faster by ingesting, parsing, normalizing, and auditing **X12 837 claims** in Databricks—without building a new stored procedure or a new Talend job per partner.

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
