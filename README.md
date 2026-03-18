🏥 MediCore Hospital Data Platform

End-to-End Snowflake Data Engineering Pipeline with Real-Time Hospital Analytics Dashboard

MediCore is a modern healthcare data platform built on Snowflake + AWS S3 + Streamlit, designed to ingest, validate, transform, and analyze hospital operational data in real time.

The system implements a complete Medallion Architecture (RAW → VALIDATED → CURATED) with automated ingestion, anomaly detection, security policies, and KPI dashboards.

🚀 Project Overview

Healthcare organizations generate large volumes of operational data such as:

Patient registrations

Doctor information

Admissions

Surgical procedures

Billing records

MediCore transforms these raw operational datasets into analytics-ready data models for operational intelligence.

The platform enables:

Real-time anomaly detection

Hospital performance KPIs

Doctor workload analysis

Bed utilization analytics

Billing accuracy monitoring

🏗️ Architecture
AWS S3
   │
   ▼
Snowflake External Stage
   │
   ▼
Snowpipe (Auto Ingest)
   │
   ▼
RAW Layer (Bronze)
   │
   ▼
VALIDATED Layer (Silver)
   │
   ▼
CURATED Layer (Gold / Star Schema)
   │
   ▼
Streamlit Dashboard
🧱 Data Layers
1️⃣ RAW Layer (Bronze)

Stores unaltered source data from S3.

Tables:

PATIENTS_RAW

DOCTORS_RAW

ADMISSIONS_RAW

PROCEDURES_RAW

BILLING_RAW

Features:

Loaded via Snowpipe auto-ingestion

Uses external stage connected to AWS S3

CSV ingestion using Snowflake COPY INTO

Purpose:

Maintain original source data

Provide audit traceability

2️⃣ VALIDATED Layer (Silver)

Performs data quality validation and cleansing.

Data Quality Checks
Check Type	Description
Referential Integrity	patient_id and doctor_id must exist
Temporal Validation	discharge_time ≥ admission_time
Value Validation	bed_no must be positive
Billing Validation	amounts cannot be negative
Deduplication	ROW_NUMBER based record selection
Tables

PATIENTS_VALIDATED

DOCTORS_VALIDATED

ADMISSIONS_VALIDATED

PROCEDURES_VALIDATED

BILLING_VALIDATED

🔎 Data Quality Monitoring
DQ Failure Log

DQ_FAILURE_LOG

Tracks:

Invalid timestamps

Missing references

Negative billing values

Invalid bed numbers

Example fields:

table_name
record_id
issue_type
issue_description
created_at
⚠️ Anomaly Detection Engine

Multiple stored procedures detect operational anomalies.

Procedure	Description
DETECT_BED_CONFLICT	Same bed assigned to multiple patients
DETECT_PROCEDURE_CLASH	Surgeon scheduled in overlapping surgeries
DETECT_OT_OVERRUN	Surgeries exceeding 180 minutes
DETECT_MULTI_ADMISSIONS	Patient admitted simultaneously
DETECT_BILLING_ANOMALY	Abnormally high billing

Master Procedure:

RUN_ALL_ANOMALIES()

This runs all anomaly detection jobs automatically.

⭐ CURATED Layer (Gold)

The curated layer builds a star schema for analytics.

Dimension Tables
DIM_PATIENTS

SCD Type-2 implementation

Tracks patient history changes.

Columns:

patient_sk
patient_id
name
email
phone
city
state
insurance_id
effective_start_date
effective_end_date
is_current
DIM_DOCTORS

SCD Type-2 implementation

Tracks doctor department and status changes.

DIM_DATE

Calendar dimension used for analytics.

Fact Tables
FACT_ADMISSIONS

Links:

patient_sk
doctor_sk
admission_time
discharge_time
department
ward
bed_no
FACT_PROCEDURES

Tracks surgical procedures.

FACT_BILLING

Tracks hospital billing events.

📊 KPI Metrics

The platform calculates operational KPIs used by hospital management.

KPI 1 — Bed Utilization
Occupied Bed Hours / Total Beds

Measures hospital infrastructure usage.

KPI 2 — Admission Turnaround Time
Average (Discharge Time − Admission Time)

Indicates treatment duration.

KPI 3 — Surgery Efficiency
% Surgeries starting within ±10 minutes of schedule

Measures OT operational efficiency.

KPI 4 — Doctor Workload Index
Admissions + Procedures per doctor

Evaluates doctor workload distribution.

KPI 5 — Billing Accuracy
1 - (Billing Anomalies / Total Bills)

Detects financial irregularities.

⚡ Incremental Data Processing

Incremental pipeline is handled using:

Streams
PATIENT_STREAM
DOCTOR_STREAM
ADMISSION_STREAM
PROCEDURE_STREAM
BILLING_STREAM

These capture change data from RAW tables.

Tasks

Scheduled tasks automate the pipeline.

Task	Schedule
Incremental Pipeline	Every 5 minutes
KPI Refresh	Every 10 minutes

Pipeline Steps:

Load incremental data

Apply SCD2 updates

Run anomaly detection

Update KPIs

🔐 Security Implementation

The platform implements enterprise-grade data security.

1️⃣ Dynamic Data Masking

Sensitive fields are protected:

Masked columns:

email
phone
insurance_id

Example:

Role	Result
SYSADMIN	Full value
Other roles	Masked value
2️⃣ Row Level Security (RLS)

Access restricted by department.

Roles:

CARDIO_ROLE
ORTHO_ROLE
ER_ROLE
IPD_ROLE
OPD_ROLE

Example:

ER_ROLE → Can only see ER department records
📊 Streamlit Dashboard

The dashboard is deployed inside Snowflake using Streamlit.

Features:

Real-time KPI monitoring

Department filters

Doctor workload charts

OT analytics

Billing analytics

Anomaly detection logs

Data quality failure logs

Pipeline monitoring

Dashboard Sections:

KPI Cards
Admissions Analytics
Operation Theatre Analytics
Doctor Workload
Billing Analytics
Anomaly Logs
Data Quality Logs
Pipeline Layer Monitoring
Load Audit Logs
📦 Technologies Used
Technology	Purpose
Snowflake	Data warehouse
AWS S3	Data storage
Snowpipe	Automated ingestion
Streams	Change Data Capture
Tasks	Pipeline automation
SQL	Data transformation
Streamlit	Interactive dashboard
Snowpark	Streamlit Snowflake integration
📁 Project Structure
medicore-hospital-platform
│
├── sql
│   ├── setup.sql
│   ├── ingestion.sql
│   ├── validation.sql
│   ├── anomaly_detection.sql
│   ├── curated_model.sql
│   └── security.sql
│
├── streamlit
│   └── app.py
│
├── data
│   ├── patients.csv
│   ├── doctors.csv
│   ├── admissions.csv
│   ├── procedures.csv
│   └── billing.csv
│
└── README.md
📈 Example Dashboard Insights

The dashboard provides insights such as:

Top departments by admissions

OT rooms with highest usage

Surgeons with highest workload

High-value billing transactions

Operational anomalies

🔄 End-to-End Pipeline Flow
CSV Data → AWS S3
        ↓
Snowpipe Auto Ingestion
        ↓
RAW Layer (Bronze)
        ↓
VALIDATED Layer (Data Quality + Deduplication)
        ↓
Anomaly Detection
        ↓
CURATED Layer (Star Schema)
        ↓
KPI Views
        ↓
Streamlit Dashboard
🎯 Key Highlights

✔ Fully automated Snowflake pipeline
✔ Medallion architecture implementation
✔ Real-time anomaly detection
✔ SCD Type-2 dimensional modeling
✔ Enterprise-grade data security
✔ Interactive analytics dashboard

**KPI'S**
<img width="1785" height="625" alt="image" src="https://github.com/user-attachments/assets/b72745d5-b10f-48dc-bf37-957df50500e7" />

**Streamlit App deployed**
**https://medi-core-dashboard-fhjxtfdk8bv32rcipizndg.streamlit.app/**
