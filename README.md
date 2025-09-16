# Azure + Databricks Data Engineering Project (Healthcare RCM)

This project demonstrates a full-stack Azure Data Engineering pipeline, using real-world healthcare Revenue Cycle Management (RCM) scenarios. The solution leverages Azure Data Factory, Databricks, Delta Lake, and ADLS Gen2 to ingest, transform, and deliver clean, analytics-ready data for KPIs like Accounts Receivable aging, days in AR, and provider benchmarks. All datasets are synthetic.

The goal is to build a unified, reliable analytics solution for hospital revenue by integrating EMR, insurance claims, and code-set data into clean, governed Fact and Dimension tables—enabling finance and revenue integrity teams to consistently track AR aging, Days in AR, denials, and cash collection trends across hospitals while reducing time-to-insight and improving decision-making.

---

## 📌 What this project demonstrates

- Cloud‑native **data lakehouse** on Azure following **Landing → Bronze → Silver → Gold**
- **Config‑driven ingestion** from multiple sources (Azure SQL, flat files, public APIs)
- **Incremental & full loads** with watermarking + **audit logging**
- **Databricks** ETL with **Delta Lake**, **SCD Type 2** dimensions, **CDM** standardization, and **data quality quarantine**
- **Star schema** in Gold layer with **Fact**/**Dim** tables for RCM KPIs
- **Azure Key Vault** secrets integration and **Unity Catalog** governance
- **ADF** orchestration with **ForEach** parallelization and retries

---

## 🗺️ Solution Architecture

<img width="1178" height="693" alt="project_architecture" src="https://github.com/user-attachments/assets/fdc30b55-18e1-4c10-8c9b-406533c40f0b" />
<br>

- **Landing** → raw drops (CSV, JSON)
- **Bronze (Parquet)** → source‑of‑truth, minimally processed
- **Silver (Delta)** → cleansed, CDM, SCD2, quality checks
- **Gold (Delta)** → star schema, KPI‑ready fact/dim

**Services**: Azure SQL Database (EMR), ADLS Gen2, Azure Databricks, ADF, Key Vault, Unity Catalog

**High‑level flow**:
1. **EMR (Azure SQL)**, **Claims (CSV)**, **NPI/ICD (APIs)**, **CPT (CSV)** → **Landing** / **Bronze**
2. ADF **Lookup** (config) → **ForEach** (per entity) → **Copy** (to Bronze Parquet) → **Audit log**
3. Databricks **Bronze → Silver** (clean/CDM/SCD2/quarantine)
4. Databricks **Silver → Gold** (Facts/Dimensions)
5. BI/ML consumers hit **Gold**/**Silver** respectively


---

## Key Features
- **Azure Data Factory**: Orchestrates ingestion, applies incremental/full-load logic using metadata-driven config files.
- **Delta Lake ACID Transactions**: Allows upserts/SCD2 in Silver/Gold layers.
- **ADLS Gen2**: Data lake architecture with containers for landing, bronze, silver, gold, config metadata.
- **Databricks (PySpark/Spark SQL)**: Transformation, enrichment and CDM logic.
- **Audit Logging**: All loads tracked in audit tables for data lineage & traceability.
- **Key Vault**: Secure management of secrets and credentials.
- **Parallelization**: Data engineering pipelines made parallel for scale using ADF.
- **Unity Catalog**: Migration from Hive metastore to Unity for robust data governance.
- **Best Practices**: Active/inactive flags, naming conventions, pipeline retries, folder structure optimization.

<img width="1626" height="633" alt="image" src="https://github.com/user-attachments/assets/2ef92acd-7bc1-4b12-8dce-2df186ff7e8f" />

---

## 📂 Data & Sources

- **EMR Data (Azure SQL DB)**: Hospital patient, provider, transaction, department, encounter tables (from Bangalore clinics, hospitals).

- **Claims Data (Flat Files in ADLS Gen2)**: Insurance claim information, delivered monthly.

- **NPI Data (Public API)**: National Provider Identifier details for healthcare providers.

- **ICD Data (Public API)**: Standardized diagnosis codes, mapped for analytics.

⚠️ **Note**: Data is generated using `faker`; some joins will not match perfectly and API pulls are intentionally sparse.

---

## 🗃️ Storage layout (ADLS Gen2)

<img width="1628" height="619" alt="image" src="https://github.com/user-attachments/assets/6ea3a2de-d704-417f-9036-1beb4336aa4b" />

The Storage account is `aradlsdev`) with containers:
```
landing/
bronze/
silver/
gold/
configs/
  └── emr/
      └── load_config.csv
```

Sample **config file** (`configs/emr/load_config.csv`):
```
database,datasource,tablename,loadtype,watermark,is_active,targetpath
ar-hospital-a,hos-a,dbo.encounters,Incremental,ModifiedDate,1,hosa
ar-hospital-a,hos-a,dbo.patients,Incremental,ModifiedDate,1,hosa
ar-hospital-a,hos-a,dbo.transactions,Incremental,ModifiedDate,1,hosa
ar-hospital-a,hos-a,dbo.providers,Full,,1,hosa
ar-hospital-a,hos-a,dbo.departments,Full,,1,hosa
ar-hospital-b,hos-b,dbo.encounters,Incremental,ModifiedDate,1,hosb
ar-hospital-b,hos-b,dbo.patients,Incremental,Updated_Date,1,hosb
ar-hospital-b,hos-b,dbo.transactions,Incremental,ModifiedDate,1,hosb
ar-hospital-b,hos-b,dbo.providers,Full,,1,hosb
ar-hospital-b,hos-b,dbo.departments,Full,,1,hosb
```

---

## 🧩 Repository structure
```
├─ adf/
│  ├─ dataset/
│  │  ├─ databricks_deltalake_ds.json
│  │  ├─ generic_adls_flat_file_ds.json
│  │  ├─ generic_adls_parquest_ds.json
│  │  └─ generic_sql_ds.json
│  ├─ factory/
│  │  └─ arhospital-adf.json
│  ├─ linkedService/
│  │  ├─ ar_hc_adb_ls.json
│  │  ├─ ar_hc_adls_ls.json
│  │  ├─ ar_hc_dl_ls.json
│  │  ├─ ar_hc_kv_ls.json
│  │  └─ hosa_sqlDB_ls.json
│  ├─ pipeline/
│  │  ├─ pl_emr_src_to_landing.json
│  │  ├─ pl_end_to_end_hc.json
│  │  └─ pl_silver_to_gold.json
│  └─ publish_config.json
├─ databricks/
│  ├─ API extracts/
│  │  ├─ ICD Code API extract.ipynb
│  │  └─ NPI API extract.ipynb
│  ├─ Gold/
│  │  ├─ dim_cpt_code.ipynb
│  │  ├─ dim_department.ipynb
│  │  ├─ dim_icd_code.ipynb
│  │  ├─ dim_npi.ipynb
│  │  ├─ dim_patient.ipynb
│  │  ├─ dim_provider.ipynb
│  │  └─ fact_transaction.ipynb
│  ├─ Set up/
│  │  ├─ adls_mount.ipynb
│  │  └─ audit_ddl.ipynb
│  └─ Silver/
│     ├─ CPT codes.ipynb
│     ├─ Claims.ipynb
│     ├─ Departments_F.ipynb
│     ├─ Encounters.ipynb
│     ├─ ICD Code.ipynb
│     ├─ NPI.ipynb
│     ├─ Patients.ipynb
│     ├─ Providers_F.ipynb
│     └─ Transactions.ipynb
├─ sqlDB_DDL/
│  ├─ hospital_a.txt
│  └─ hospital_b.txt
└─ README.md
```

---

## 🔐 Security & Secrets
- **Azure Key Vault** holds secrets (e.g., storage keys, SQL creds, SPN client secret).
- Databricks secret scope created and linked to KV.
- Example usage in notebooks:
```python
# Databricks
adls_key = dbutils.secrets.get('hc-kv-scope', 'adls-access-key-dev')
```

<img width="1628" height="516" alt="image" src="https://github.com/user-attachments/assets/5568f77a-c815-4f74-acb8-964a907ecd9b" />

---

## 🏗️ Orchestration (ADF)

### Linked Services
- Azure SQL DB
- ADLS Gen2
- Delta Lake / Databricks
- Key Vault

<img width="1631" height="441" alt="image" src="https://github.com/user-attachments/assets/cd7c48a8-b1d1-495c-827b-b8ff56bc256d" />



### ADF Datasets — reusable, parameterized building blocks

The datasets are the “connectors” the pipelines reuse for every entity. By keeping them generic and parameterized, the ForEach loop can swap sources/sinks at runtime based on the config file—no new datasets per table.

<img width="1589" height="461" alt="image" src="https://github.com/user-attachments/assets/14e99de6-2867-466d-901e-0d1d49a8292f" />


## Pipeline pattern
- **pl_emr_src_to_landing**: Config-driven ADF ingestion. Reads load_config.csv, loops entities, archives existing files, runs full or incremental (watermark) loads from Azure SQL/landing to Bronze Parquet, and writes to audit.load_logs. Parallel enabled.
<img width="1628" height="676" alt="image" src="https://github.com/user-attachments/assets/04fb32c0-f1eb-46d9-ac43-e88487d55be9" />
<br><br>


- **pl_silver_to_gold**: Orchestrates Databricks notebooks. Builds Silver (clean/CDM, DQ quarantine, SCD2) then Gold (star-schema Facts/Dims) Delta tables for analytics.
<img width="1628" height="668" alt="image" src="https://github.com/user-attachments/assets/bc94a76e-70b6-452e-908a-f4e45a311375" />
<br><br>


- **pl_end_to_end_hc**: Wrapper pipeline that executes the two stages in sequence for a single-button, schedulable end-to-end run with shared parameters.
<img width="1622" height="621" alt="image" src="https://github.com/user-attachments/assets/3e668d9e-2450-440d-b754-0e54b89dc6ce" />
<br><br>


### Audit table (Delta)
```sql
-- Databricks notebook cell
%sql
CREATE SCHEMA IF NOT EXISTS ar_hos_adb_ws.audit;

CREATE TABLE IF NOT EXISTS ar_hos_adb_ws.audit.load_logs (
    data_source STRING,
    tablename STRING,
    numberofrowscopied INT,
    watermarkcolumnname STRING,
    loaddate TIMESTAMP
);
```

### ADF dynamic content (examples)
**Read last watermark**
```
@concat(
  'select coalesce(cast(max(loaddate) as date),''',''1900-01-01'',''') as last_fetched_date ',
  'from audit.load_logs where data_source=''' , item().datasource , 
  ''' and tablename=''' , item().tablename , '''')
```

**Incremental extract**
```
@concat(
  'select *,''' , item().datasource , ''' as datasource from ' , item().tablename ,
  ' where ' , item().watermark , ' >= ''' , activity(''Fetch_logs'').output.firstRow.last_fetched_date , '''')
```

**Insert audit row**
```
@concat(
  'insert into audit.load_logs(data_source,tablename,numberofrowscopied,watermarkcolumnname,loaddate) values (''',
  item().datasource, ''', ''', item().tablename, ''',''', activity(''Incremental_Load_CP'').output.rowscopied,
  ''',''', item().watermark, ''',''', utcNow(), ''')')
```

> Swap `Incremental_Load_CP` with `Full_Load_CP` in the full‑load branch.

---

## 🧱 Bronze → Silver (Databricks)

**Goals**: clean, conform to a **Common Data Model (CDM)**, apply **quality checks**, and implement **SCD2** on change‑tracking dims.

### Quality checks & quarantine
- Null checks, domain checks, referential checks
- Add flags: `is_quarantined BOOLEAN`, `dq_fail_reason STRING`
- Quarantined rows persist in Silver but are excluded from Gold

### SCD Type 2 template (MERGE)
```sql
MERGE INTO silver.patients AS target
USING quality_checks AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = true 
WHEN MATCHED 
  AND (
    target.SRC_PatientID != source.SRC_PatientID OR
    target.FirstName != source.FirstName OR
    target.LastName != source.LastName OR
    target.MiddleName != source.MiddleName OR
    target.SSN != source.SSN OR
    target.PhoneNumber != source.PhoneNumber OR
    target.Gender != source.Gender OR
    target.DOB != source.DOB OR
    target.Address != source.Address OR
    target.SRC_ModifiedDate != source.ModifiedDate OR
    target.datasource != source.datasource OR 
    target.is_quarantined != source.is_quarantined
  )
THEN UPDATE SET
    is_current = false,
    modified_date = current_timestamp()

-- Insert new records into the Delta table
WHEN NOT MATCHED THEN
INSERT (
    Patient_Key, SRC_PatientID, FirstName, LastName, MiddleName, SSN, PhoneNumber,
    Gender, DOB, Address, SRC_ModifiedDate, datasource, is_quarantined,
    inserted_date, modified_date, is_current
)
VALUES (
    source.Patient_Key, source.SRC_PatientID, source.FirstName, source.LastName, source.MiddleName,
    source.SSN, source.PhoneNumber, source.Gender, source.DOB, source.Address, source.ModifiedDate,
    source.datasource, source.is_quarantined, current_timestamp(), current_timestamp(), true
);

```

---

## ⭐ Silver → Gold (Star schema)

### Dimensions (SCD2 where noted)
- `dim_patient` (SCD2)
- `dim_provider` (full refresh)
- `dim_department` (full refresh)
- `dim_date`
- `dim_payer` / `dim_plan`
- `dim_cpt`, `dim_icd`, `dim_npi`

### Facts
- `fact_encounter`
- `fact_transactions`
- `fact_claim`
- `fact_ar` (daily snapshot of outstanding receivables)

---

## 🧭 Unity Catalog
- Moved from local Hive metastore → **Unity Catalog** (`catalog.schema.table`)
- Example naming: `ar_hos_adb_ws.silver.patients`, `ar_hos_adb_ws.gold.fact_transaction`
- Managed permissions per workspace/group; enable lineage

<img width="1631" height="823" alt="image" src="https://github.com/user-attachments/assets/dedf63bb-e8a5-49da-b50d-af7f63d2919d" />

---

---
