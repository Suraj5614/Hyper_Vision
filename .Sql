-- ============================================================
-- MEDICORE HOSPITAL DATA PLATFORM — FIXED & COMPLETE SCRIPT
-- Use Case: Hospital Healthcare Operations (5 Source Files)
-- ============================================================

-- ============================================================
-- SECTION 1: SETUP — WAREHOUSE, DATABASE, SCHEMAS
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS HOSPITAL_WH
WITH
    WAREHOUSE_SIZE   = 'MEDIUM'
    AUTO_SUSPEND     = 300
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE HOSPITAL_WH;

USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE MEDICORE_DB;
USE DATABASE MEDICORE_DB;

CREATE SCHEMA RAW;
CREATE SCHEMA VALIDATED;
CREATE SCHEMA CURATED;
CREATE SCHEMA TESTING;

-- ============================================================
-- SECTION 2: S3 INTEGRATION & STAGE
-- ============================================================

USE SCHEMA MEDICORE_DB.RAW;

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION MEDICORE_S3_INT
TYPE                      = EXTERNAL_STAGE
STORAGE_PROVIDER          = S3
ENABLED                   = TRUE
STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::730335644507:role/snowflake_s3_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://medicore-hospital-data/');

DESC INTEGRATION MEDICORE_S3_INT;

CREATE OR REPLACE STAGE MEDICORE_S3_STAGE
    STORAGE_INTEGRATION = MEDICORE_S3_INT
    URL                 = 's3://medicore-hospital-data/';

LIST @MEDICORE_S3_STAGE;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE                        = CSV
    FIELD_DELIMITER             = ','
    SKIP_HEADER                 = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                     = ('NULL', 'null', '');

-- ============================================================
-- SECTION 3: RAW LAYER (Bronze) — Table Definitions
-- ============================================================

USE SCHEMA MEDICORE_DB.RAW;

CREATE OR REPLACE TABLE PATIENTS_RAW (
    patient_id        STRING,
    name              STRING,
    dob               DATE,
    gender            STRING,
    email             STRING,
    phone             STRING,
    address           STRING,
    city              STRING,
    state             STRING,
    insurance_id      STRING,
    registration_date DATE
);

CREATE OR REPLACE TABLE DOCTORS_RAW (
    doctor_id      STRING,
    name           STRING,
    specialization STRING,
    join_date      DATE,
    phone          STRING,
    email          STRING,
    qualification  STRING,
    department     STRING,
    status         STRING
);

CREATE OR REPLACE TABLE ADMISSIONS_RAW (
    admission_id        STRING,
    patient_id          STRING,
    admission_time      TIMESTAMP,
    discharge_time      TIMESTAMP,
    department          STRING,
    ward                STRING,
    bed_no              INTEGER,
    attending_doctor_id STRING,
    admission_type      STRING,
    status              STRING
);

CREATE OR REPLACE TABLE PROCEDURES_RAW (
    procedure_id    STRING,
    admission_id    STRING,
    patient_id      STRING,
    procedure_name  STRING,
    surgeon_id      STRING,
    scheduled_time  TIMESTAMP,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    operating_room  STRING,
    anaesthesia_type STRING,
    status          STRING
);

CREATE OR REPLACE TABLE BILLING_RAW (
    bill_id              STRING,
    admission_id         STRING,
    patient_id           STRING,
    total_amount         NUMBER(10,2),
    insurance_amount     NUMBER(10,2),
    out_of_pocket_amount NUMBER(10,2),
    billing_time         TIMESTAMP,
    payment_mode         STRING,
    is_flagged           BOOLEAN
);

-- ============================================================
-- SECTION 4: INGEST — COPY INTO RAW TABLES
-- ============================================================

COPY INTO PATIENTS_RAW
FROM @MEDICORE_S3_STAGE/patients/patients_master.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO DOCTORS_RAW
FROM @MEDICORE_S3_STAGE/doctors/doctors_master.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO ADMISSIONS_RAW
FROM @MEDICORE_S3_STAGE/admissions/admissions_master.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO PROCEDURES_RAW
FROM @MEDICORE_S3_STAGE/procedures/procedures_master.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO BILLING_RAW
FROM @MEDICORE_S3_STAGE/billing/billing_master.csv
FILE_FORMAT = CSV_FORMAT;

-- ============================================================
-- SECTION 5: VALIDATED LAYER (Silver)
-- FIX: Explicitly select columns (exclude rn) to avoid
--      leaking the row_number column into downstream layers
-- ============================================================

USE SCHEMA MEDICORE_DB.VALIDATED;

-- 5.1 PATIENTS_VALIDATED — deduplicate on patient_id
CREATE OR REPLACE TABLE PATIENTS_VALIDATED AS
SELECT
    patient_id,
    name,
    dob,
    gender,
    email,
    phone,
    address,
    city,
    state,
    insurance_id,
    registration_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY registration_date DESC) AS rn
    FROM MEDICORE_DB.RAW.PATIENTS_RAW
)
WHERE rn = 1;

-- 5.2 DOCTORS_VALIDATED — deduplicate on doctor_id
CREATE OR REPLACE TABLE DOCTORS_VALIDATED AS
SELECT
    doctor_id,
    name,
    specialization,
    join_date,
    phone,
    email,
    qualification,
    department,
    status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY doctor_id ORDER BY join_date DESC) AS rn
    FROM MEDICORE_DB.RAW.DOCTORS_RAW
)
WHERE rn = 1;

-- 5.3 ADMISSIONS_VALIDATED
--   Referential integrity: patient_id ∈ patients, attending_doctor_id ∈ doctors
--   Temporal: discharge_time >= admission_time
--   Value: bed_no > 0
CREATE OR REPLACE TABLE ADMISSIONS_VALIDATED AS
SELECT
    a.admission_id,
    a.patient_id,
    a.admission_time,
    a.discharge_time,
    a.department,
    a.ward,
    a.bed_no,
    a.attending_doctor_id,
    a.admission_type,
    a.status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY admission_id ORDER BY admission_time DESC) AS rn
    FROM MEDICORE_DB.RAW.ADMISSIONS_RAW
) a
INNER JOIN MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED p
    ON a.patient_id = p.patient_id
INNER JOIN MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED d
    ON a.attending_doctor_id = d.doctor_id
WHERE
    a.discharge_time >= a.admission_time
    AND a.bed_no > 0
    AND a.rn = 1;

-- 5.4 PROCEDURES_VALIDATED
--   Referential integrity: admission_id ∈ admissions
--   Temporal: start_time >= scheduled_time, end_time >= start_time
CREATE OR REPLACE TABLE PROCEDURES_VALIDATED AS
SELECT
    pr.procedure_id,
    pr.admission_id,
    pr.patient_id,
    pr.procedure_name,
    pr.surgeon_id,
    pr.scheduled_time,
    pr.start_time,
    pr.end_time,
    pr.operating_room,
    pr.anaesthesia_type,
    pr.status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY procedure_id ORDER BY scheduled_time DESC) AS rn
    FROM MEDICORE_DB.RAW.PROCEDURES_RAW
) pr
INNER JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a
    ON pr.admission_id = a.admission_id
WHERE
    pr.start_time >= pr.scheduled_time
    AND pr.end_time >= pr.start_time
    AND pr.rn = 1;

-- 5.5 BILLING_VALIDATED
--   Referential integrity: admission_id ∈ admissions
--   Temporal: billing_time >= admission_time
--   Value: all amounts >= 0
CREATE OR REPLACE TABLE BILLING_VALIDATED AS
SELECT
    b.bill_id,
    b.admission_id,
    b.patient_id,
    b.total_amount,
    b.insurance_amount,
    b.out_of_pocket_amount,
    b.billing_time,
    b.payment_mode,
    b.is_flagged
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY bill_id ORDER BY billing_time DESC) AS rn
    FROM MEDICORE_DB.RAW.BILLING_RAW
) b
INNER JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a
    ON b.admission_id = a.admission_id
WHERE
    b.total_amount         >= 0
    AND b.insurance_amount >= 0
    AND b.out_of_pocket_amount >= 0
    AND b.billing_time >= a.admission_time
    AND b.rn = 1;

-- ============================================================
-- SECTION 6: EXCEPTION / LOGGING TABLES
-- ============================================================

USE SCHEMA MEDICORE_DB.VALIDATED;

-- DQ Failure log
CREATE OR REPLACE TABLE DQ_FAILURE_LOG (
    id                INTEGER AUTOINCREMENT,
    table_name        STRING,
    record_id         STRING,
    issue_type        STRING,
    issue_description STRING,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Anomaly Log
CREATE OR REPLACE TABLE ANOMALY_LOG (
    id            INTEGER AUTOINCREMENT,
    anomaly_type  STRING,
    reference_id  STRING,
    description   STRING,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Load Audit Log
CREATE OR REPLACE TABLE LOAD_AUDIT_LOG (
    table_name     STRING,
    load_time      TIMESTAMP,
    records_loaded INTEGER
);

-- ============================================================
-- SECTION 7: POPULATE LOAD_AUDIT_LOG FOR ALL 5 TABLES
-- ============================================================

INSERT INTO MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG (table_name, load_time, records_loaded)
SELECT 'PATIENTS',   CURRENT_TIMESTAMP(), COUNT(*) FROM MEDICORE_DB.RAW.PATIENTS_RAW;

INSERT INTO MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG (table_name, load_time, records_loaded)
SELECT 'DOCTORS',    CURRENT_TIMESTAMP(), COUNT(*) FROM MEDICORE_DB.RAW.DOCTORS_RAW;

INSERT INTO MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG (table_name, load_time, records_loaded)
SELECT 'ADMISSIONS', CURRENT_TIMESTAMP(), COUNT(*) FROM MEDICORE_DB.RAW.ADMISSIONS_RAW;

INSERT INTO MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG (table_name, load_time, records_loaded)
SELECT 'PROCEDURES', CURRENT_TIMESTAMP(), COUNT(*) FROM MEDICORE_DB.RAW.PROCEDURES_RAW;

INSERT INTO MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG (table_name, load_time, records_loaded)
SELECT 'BILLING',    CURRENT_TIMESTAMP(), COUNT(*) FROM MEDICORE_DB.RAW.BILLING_RAW;

SELECT * FROM MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG;

-- ============================================================
-- SECTION 8: DQ FAILURE LOGGING — Initial Checks
-- ============================================================

-- Discharge before admission (temporal violation)
INSERT INTO MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
    (table_name, record_id, issue_type, issue_description)
SELECT
    'ADMISSIONS',
    admission_id,
    'INVALID_TIME',
    'Discharge before admission'
FROM MEDICORE_DB.RAW.ADMISSIONS_RAW
WHERE discharge_time < admission_time;

-- Missing patient reference
INSERT INTO MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
    (table_name, record_id, issue_type, issue_description)
SELECT
    'ADMISSIONS',
    a.admission_id,
    'MISSING_PATIENT',
    'Patient ID not found in validated patients'
FROM MEDICORE_DB.RAW.ADMISSIONS_RAW a
LEFT JOIN MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED p
    ON a.patient_id = p.patient_id
WHERE p.patient_id IS NULL;

-- Missing doctor reference
INSERT INTO MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
    (table_name, record_id, issue_type, issue_description)
SELECT
    'ADMISSIONS',
    a.admission_id,
    'MISSING_DOCTOR',
    'Doctor ID not found in validated doctors'
FROM MEDICORE_DB.RAW.ADMISSIONS_RAW a
LEFT JOIN MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED d
    ON a.attending_doctor_id = d.doctor_id
WHERE d.doctor_id IS NULL;

-- Invalid bed number
INSERT INTO MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
    (table_name, record_id, issue_type, issue_description)
SELECT
    'ADMISSIONS',
    admission_id,
    'INVALID_BED_NO',
    'Bed number must be positive'
FROM MEDICORE_DB.RAW.ADMISSIONS_RAW
WHERE bed_no <= 0 OR bed_no IS NULL;

-- Negative billing amounts
INSERT INTO MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
    (table_name, record_id, issue_type, issue_description)
SELECT
    'BILLING',
    bill_id,
    'NEGATIVE_AMOUNT',
    'One or more billing amounts are negative'
FROM MEDICORE_DB.RAW.BILLING_RAW
WHERE total_amount < 0
   OR insurance_amount < 0
   OR out_of_pocket_amount < 0;

-- ============================================================
-- SECTION 9: ANOMALY DETECTION STORED PROCEDURES
-- ============================================================

USE SCHEMA MEDICORE_DB.VALIDATED;

-- 9.1 Bed Conflict Detection
CREATE OR REPLACE PROCEDURE DETECT_BED_CONFLICT()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT DISTINCT
        'BED_CONFLICT',
        a1.admission_id,
        'Same bed assigned to multiple patients at overlapping time'
    FROM MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a1
    JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a2
        ON  a1.bed_no        = a2.bed_no
        AND a1.ward          = a2.ward          -- same ward + bed = same physical bed
        AND a1.admission_id <> a2.admission_id
        AND a1.admission_time < a2.discharge_time
        AND a2.admission_time < a1.discharge_time;

    RETURN 'Bed conflict detection completed';
END;
$$;

-- 9.2 Procedure Schedule Clash (surgeon overlap)
CREATE OR REPLACE PROCEDURE DETECT_PROCEDURE_CLASH()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT DISTINCT
        'PROCEDURE_CLASH',
        p1.procedure_id,
        'Surgeon has overlapping procedures'
    FROM MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED p1
    JOIN MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED p2
        ON  p1.surgeon_id    = p2.surgeon_id
        AND p1.procedure_id <> p2.procedure_id
        AND p1.start_time    < p2.end_time
        AND p2.start_time    < p1.end_time;

    RETURN 'Procedure clash detection completed';
END;
$$;

-- 9.3 Operation Theatre Overrun (> 180 minutes)
CREATE OR REPLACE PROCEDURE DETECT_OT_OVERRUN()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT
        'OT_OVERRUN',
        procedure_id,
        'Procedure duration exceeded 180-minute threshold'
    FROM MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED
    WHERE DATEDIFF(MINUTE, start_time, end_time) > 180;

    RETURN 'OT overrun detection completed';
END;
$$;

-- 9.4 Multiple Active Admissions (same patient overlapping)
CREATE OR REPLACE PROCEDURE DETECT_MULTI_ADMISSIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT DISTINCT
        'MULTIPLE_ADMISSIONS',
        a1.patient_id,
        'Patient has overlapping active admissions'
    FROM MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a1
    JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a2
        ON  a1.patient_id    = a2.patient_id
        AND a1.admission_id <> a2.admission_id
        AND a1.admission_time < a2.discharge_time
        AND a2.admission_time < a1.discharge_time;

    RETURN 'Multiple admissions detection completed';
END;
$$;

-- 9.5 Billing Anomalies
--   FIX: Use DEPARTMENT average (not global average) per use case requirement
CREATE OR REPLACE PROCEDURE DETECT_BILLING_ANOMALY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- High billing vs department average (use case: unusually high vs department average)
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT
        'HIGH_BILLING',
        b.bill_id,
        'Billing significantly higher than department average (3x threshold)'
    FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED b
    JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a
        ON b.admission_id = a.admission_id
    JOIN (
        SELECT
            a2.department,
            AVG(b2.total_amount) * 3 AS threshold
        FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED b2
        JOIN MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a2
            ON b2.admission_id = a2.admission_id
        GROUP BY a2.department
    ) dept_avg
        ON a.department = dept_avg.department
    WHERE b.total_amount > dept_avg.threshold;

    -- Duplicate billing: same patient billed multiple times at same timestamp
    INSERT INTO MEDICORE_DB.VALIDATED.ANOMALY_LOG
        (anomaly_type, reference_id, description)
    SELECT
        'DUPLICATE_BILL_TIME',
        b.patient_id,
        'Multiple bills recorded at the same billing_time for one patient'
    FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED b
    GROUP BY b.patient_id, b.billing_time
    HAVING COUNT(*) > 1;

    RETURN 'Billing anomaly detection completed';
END;
$$;

-- 9.6 Master Procedure — Run All Anomaly Checks
CREATE OR REPLACE PROCEDURE RUN_ALL_ANOMALIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL DETECT_BED_CONFLICT();
    CALL DETECT_PROCEDURE_CLASH();
    CALL DETECT_OT_OVERRUN();
    CALL DETECT_MULTI_ADMISSIONS();
    CALL DETECT_BILLING_ANOMALY();
    RETURN 'All anomaly checks executed successfully';
END;
$$;

-- Run anomaly checks
CALL RUN_ALL_ANOMALIES();
SELECT * FROM MEDICORE_DB.VALIDATED.ANOMALY_LOG;

-- ============================================================
-- SECTION 10: TESTING SCHEMA — Anomaly Validation
-- ============================================================

USE SCHEMA MEDICORE_DB.TESTING;

CREATE OR REPLACE TABLE ADMISSIONS_TEST AS
SELECT * FROM MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED;

CREATE OR REPLACE TABLE PATIENTS_TEST AS
SELECT * FROM MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED;

CREATE OR REPLACE TABLE DOCTORS_TEST AS
SELECT * FROM MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED;

CREATE OR REPLACE TABLE PROCEDURES_TEST AS
SELECT * FROM MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED;

CREATE OR REPLACE TABLE BILLING_TEST AS
SELECT * FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED;

-- Insert test bed-conflict records
INSERT INTO MEDICORE_DB.TESTING.ADMISSIONS_TEST (
    admission_id, patient_id, admission_time, discharge_time,
    department, ward, bed_no, attending_doctor_id, admission_type, status
)
VALUES
    ('TEST_ADM1','P100','2024-01-01 10:00','2024-01-02 10:00','CARDIO','W1',101,'D1','IPD','ACTIVE'),
    ('TEST_ADM2','P101','2024-01-01 11:00','2024-01-02 09:00','CARDIO','W1',101,'D2','IPD','ACTIVE');

-- Verify bed conflict logic
SELECT
    a1.admission_id AS admission_1,
    a2.admission_id AS admission_2,
    a1.bed_no
FROM MEDICORE_DB.TESTING.ADMISSIONS_TEST a1
JOIN MEDICORE_DB.TESTING.ADMISSIONS_TEST a2
    ON  a1.bed_no        = a2.bed_no
    AND a1.admission_id <> a2.admission_id
    AND a1.admission_time < a2.discharge_time
    AND a2.admission_time < a1.discharge_time;

SELECT * FROM MEDICORE_DB.TESTING.ADMISSIONS_TEST WHERE admission_id LIKE 'TEST%';

-- ============================================================
-- SECTION 11: CURATED LAYER (Gold) — Star Schema
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE MEDICORE_DB;
USE SCHEMA MEDICORE_DB.CURATED;

-- 11.1 DIM_PATIENTS (SCD Type-2)
CREATE OR REPLACE TABLE DIM_PATIENTS (
    patient_sk           INTEGER AUTOINCREMENT,
    patient_id           STRING,
    name                 STRING,
    dob                  DATE,
    gender               STRING,
    email                STRING,
    phone                STRING,
    city                 STRING,
    state                STRING,
    insurance_id         STRING,
    effective_start_date TIMESTAMP,
    effective_end_date   TIMESTAMP,
    is_current           BOOLEAN
);

INSERT INTO MEDICORE_DB.CURATED.DIM_PATIENTS (
    patient_id, name, dob, gender, email, phone,
    city, state, insurance_id,
    effective_start_date, effective_end_date, is_current
)
SELECT
    patient_id, name, dob, gender, email, phone,
    city, state, insurance_id,
    CURRENT_TIMESTAMP(), NULL, TRUE
FROM MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED;

-- 11.2 DIM_DOCTORS (SCD Type-2)
CREATE OR REPLACE TABLE DIM_DOCTORS (
    doctor_sk            INTEGER AUTOINCREMENT,
    doctor_id            STRING,
    name                 STRING,
    specialization       STRING,
    department           STRING,
    qualification        STRING,
    status               STRING,
    effective_start_date TIMESTAMP,
    effective_end_date   TIMESTAMP,
    is_current           BOOLEAN
);

SELECT
    patient_sk,
    patient_id,
    name,
    email,
    phone,
    city,
    state,
    insurance_id,
    effective_start_date,
    effective_end_date,
    is_current
FROM MEDICORE_DB.CURATED.DIM_PATIENTS
ORDER BY patient_id, effective_start_date;

INSERT INTO MEDICORE_DB.CURATED.DIM_DOCTORS (
    doctor_id, name, specialization, department, qualification, status,
    effective_start_date, effective_end_date, is_current
)
SELECT
    doctor_id, name, specialization, department, qualification, status,
    CURRENT_TIMESTAMP(), NULL, TRUE
FROM MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED;

-- 11.3 DIM_DATE
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    DATEADD(DAY, SEQ4(), '2023-01-01')::DATE AS date_key,
    YEAR(DATEADD(DAY, SEQ4(), '2023-01-01'))  AS year,
    MONTH(DATEADD(DAY, SEQ4(), '2023-01-01')) AS month,
    DAY(DATEADD(DAY, SEQ4(), '2023-01-01'))   AS day
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

-- 11.4 FACT_ADMISSIONS — links patients + doctors + admissions
CREATE OR REPLACE TABLE FACT_ADMISSIONS AS
SELECT
    a.admission_id,
    p.patient_sk,
    d.doctor_sk,
    a.admission_time,
    a.discharge_time,
    a.department,
    a.ward,
    a.bed_no,
    a.admission_type,
    a.status
FROM MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED a
JOIN MEDICORE_DB.CURATED.DIM_PATIENTS p
    ON a.patient_id = p.patient_id AND p.is_current = TRUE
JOIN MEDICORE_DB.CURATED.DIM_DOCTORS d
    ON a.attending_doctor_id = d.doctor_id AND d.is_current = TRUE;

-- 11.5 FACT_PROCEDURES
--   Joins to DIM_PATIENTS and DIM_DOCTORS for surrogate key linkage
CREATE OR REPLACE TABLE FACT_PROCEDURES AS
SELECT
    pr.procedure_id,
    pr.admission_id,
    p.patient_sk,
    d.doctor_sk,
    pr.procedure_name,
    pr.scheduled_time,
    pr.start_time,
    pr.end_time,
    pr.operating_room,
    pr.anaesthesia_type,
    pr.status
FROM MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED pr
JOIN MEDICORE_DB.CURATED.DIM_PATIENTS p
    ON pr.patient_id = p.patient_id AND p.is_current = TRUE
JOIN MEDICORE_DB.CURATED.DIM_DOCTORS d
    ON pr.surgeon_id = d.doctor_id AND d.is_current = TRUE;

-- 11.6 FACT_BILLING
CREATE OR REPLACE TABLE FACT_BILLING AS
SELECT
    b.bill_id,
    b.admission_id,
    p.patient_sk,
    b.total_amount,
    b.insurance_amount,
    b.out_of_pocket_amount,
    b.billing_time,
    b.payment_mode,
    b.is_flagged
FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED b
JOIN MEDICORE_DB.CURATED.DIM_PATIENTS p
    ON b.patient_id = p.patient_id AND p.is_current = TRUE;

-- 11.7 KPI_SUMMARY table (required for TASK_KPI_REFRESH)
--   FIX: Was referenced in task but never created
CREATE OR REPLACE TABLE KPI_SUMMARY (
    summary_date  DATE,
    total_admissions INTEGER
);

-- ============================================================
-- SECTION 12: INCREMENTAL LOAD STORED PROCEDURES
-- ============================================================

-- 12.1 Incremental load for PATIENTS (MERGE UPSERT)
CREATE OR REPLACE PROCEDURE MEDICORE_DB.VALIDATED.LOAD_PATIENTS_INCREMENTAL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED tgt
    USING MEDICORE_DB.RAW.PATIENTS_RAW src
    ON tgt.patient_id = src.patient_id

    WHEN MATCHED THEN UPDATE SET
        tgt.name             = src.name,
        tgt.email            = src.email,
        tgt.phone            = src.phone,
        tgt.city             = src.city,
        tgt.state            = src.state,
        tgt.address          = src.address,
        tgt.insurance_id     = src.insurance_id,
        tgt.registration_date = src.registration_date

    WHEN NOT MATCHED THEN INSERT (
        patient_id, name, dob, gender, email, phone,
        address, city, state, insurance_id, registration_date
    ) VALUES (
        src.patient_id, src.name, src.dob, src.gender, src.email, src.phone,
        src.address, src.city, src.state, src.insurance_id, src.registration_date
    );

    RETURN 'Patients incremental load completed';
END;
$$;

-- 12.2 Incremental load for DOCTORS (MERGE UPSERT)
CREATE OR REPLACE PROCEDURE MEDICORE_DB.VALIDATED.LOAD_DOCTORS_INCREMENTAL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED tgt
    USING MEDICORE_DB.RAW.DOCTORS_RAW src
    ON tgt.doctor_id = src.doctor_id

    WHEN MATCHED THEN UPDATE SET
        tgt.name           = src.name,
        tgt.specialization = src.specialization,
        tgt.department     = src.department,
        tgt.qualification  = src.qualification,
        tgt.status         = src.status,
        tgt.phone          = src.phone,
        tgt.email          = src.email

    WHEN NOT MATCHED THEN INSERT (
        doctor_id, name, specialization, join_date, phone,
        email, qualification, department, status
    ) VALUES (
        src.doctor_id, src.name, src.specialization, src.join_date, src.phone,
        src.email, src.qualification, src.department, src.status
    );

    RETURN 'Doctors incremental load completed';
END;
$$;

-- 12.3 SCD Type-2 for DIM_PATIENTS
--   FIX: Only expire records where actual data has changed
CREATE OR REPLACE PROCEDURE MEDICORE_DB.CURATED.SCD2_PATIENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Expire old records ONLY where a tracked attribute has changed
    UPDATE MEDICORE_DB.CURATED.DIM_PATIENTS d
    SET
        effective_end_date = CURRENT_TIMESTAMP(),
        is_current         = FALSE
    WHERE d.is_current = TRUE
      AND d.patient_id IN (
          SELECT v.patient_id
          FROM MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED v
          JOIN MEDICORE_DB.CURATED.DIM_PATIENTS d2
              ON v.patient_id = d2.patient_id AND d2.is_current = TRUE
          WHERE
              v.name         <> d2.name
           OR v.email        <> d2.email
           OR v.phone        <> d2.phone
           OR v.city         <> d2.city
           OR v.state        <> d2.state
           OR v.insurance_id <> d2.insurance_id
      );

    -- Step 2: Insert new version for changed or new patients
    INSERT INTO MEDICORE_DB.CURATED.DIM_PATIENTS (
        patient_id, name, dob, gender, email, phone,
        city, state, insurance_id,
        effective_start_date, effective_end_date, is_current
    )
    SELECT
        v.patient_id, v.name, v.dob, v.gender, v.email, v.phone,
        v.city, v.state, v.insurance_id,
        CURRENT_TIMESTAMP(), NULL, TRUE
    FROM MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED v
    LEFT JOIN MEDICORE_DB.CURATED.DIM_PATIENTS d
        ON v.patient_id = d.patient_id AND d.is_current = TRUE
    WHERE d.patient_id IS NULL;  -- only insert new / just-expired records

    RETURN 'SCD2 applied on DIM_PATIENTS';
END;
$$;

-- 12.4 SCD Type-2 for DIM_DOCTORS
CREATE OR REPLACE PROCEDURE MEDICORE_DB.CURATED.SCD2_DOCTORS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Expire changed records
    UPDATE MEDICORE_DB.CURATED.DIM_DOCTORS d
    SET
        effective_end_date = CURRENT_TIMESTAMP(),
        is_current         = FALSE
    WHERE d.is_current = TRUE
      AND d.doctor_id IN (
          SELECT v.doctor_id
          FROM MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED v
          JOIN MEDICORE_DB.CURATED.DIM_DOCTORS d2
              ON v.doctor_id = d2.doctor_id AND d2.is_current = TRUE
          WHERE
              v.department <> d2.department
           OR v.status     <> d2.status
           OR v.specialization <> d2.specialization
      );

    -- Step 2: Insert new version
    INSERT INTO MEDICORE_DB.CURATED.DIM_DOCTORS (
        doctor_id, name, specialization, department, qualification, status,
        effective_start_date, effective_end_date, is_current
    )
    SELECT
        v.doctor_id, v.name, v.specialization, v.department, v.qualification, v.status,
        CURRENT_TIMESTAMP(), NULL, TRUE
    FROM MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED v
    LEFT JOIN MEDICORE_DB.CURATED.DIM_DOCTORS d
        ON v.doctor_id = d.doctor_id AND d.is_current = TRUE
    WHERE d.doctor_id IS NULL;

    RETURN 'SCD2 applied on DIM_DOCTORS';
END;
$$;

-- ============================================================
-- SECTION 13: CHANGE DATA CAPTURE — STREAMS
-- ============================================================

USE SCHEMA MEDICORE_DB.RAW;

CREATE OR REPLACE STREAM PATIENT_STREAM   ON TABLE MEDICORE_DB.RAW.PATIENTS_RAW;
CREATE OR REPLACE STREAM DOCTOR_STREAM    ON TABLE MEDICORE_DB.RAW.DOCTORS_RAW;
CREATE OR REPLACE STREAM ADMISSION_STREAM ON TABLE MEDICORE_DB.RAW.ADMISSIONS_RAW;
CREATE OR REPLACE STREAM PROCEDURE_STREAM ON TABLE MEDICORE_DB.RAW.PROCEDURES_RAW;
CREATE OR REPLACE STREAM BILLING_STREAM   ON TABLE MEDICORE_DB.RAW.BILLING_RAW;

-- ============================================================
-- SECTION 14: SNOWPIPES — Auto-Ingest from S3
-- ============================================================

USE SCHEMA MEDICORE_DB.RAW;

CREATE OR REPLACE PIPE PATIENTS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO MEDICORE_DB.RAW.PATIENTS_RAW
FROM @MEDICORE_DB.RAW.MEDICORE_S3_STAGE/patients/
FILE_FORMAT = (FORMAT_NAME = MEDICORE_DB.RAW.CSV_FORMAT);

CREATE OR REPLACE PIPE DOCTORS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO MEDICORE_DB.RAW.DOCTORS_RAW
FROM @MEDICORE_DB.RAW.MEDICORE_S3_STAGE/doctors/
FILE_FORMAT = (FORMAT_NAME = MEDICORE_DB.RAW.CSV_FORMAT);

CREATE OR REPLACE PIPE ADMISSIONS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO MEDICORE_DB.RAW.ADMISSIONS_RAW
FROM @MEDICORE_DB.RAW.MEDICORE_S3_STAGE/admissions/
FILE_FORMAT = (FORMAT_NAME = MEDICORE_DB.RAW.CSV_FORMAT);

CREATE OR REPLACE PIPE PROCEDURES_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO MEDICORE_DB.RAW.PROCEDURES_RAW
FROM @MEDICORE_DB.RAW.MEDICORE_S3_STAGE/procedures/
FILE_FORMAT = (FORMAT_NAME = MEDICORE_DB.RAW.CSV_FORMAT);

CREATE OR REPLACE PIPE BILLING_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO MEDICORE_DB.RAW.BILLING_RAW
FROM @MEDICORE_DB.RAW.MEDICORE_S3_STAGE/billing/
FILE_FORMAT = (FORMAT_NAME = MEDICORE_DB.RAW.CSV_FORMAT);

SHOW PIPES;
DESC PIPE PATIENTS_PIPE;

-- Check pipe status and copy history
SELECT SYSTEM$PIPE_STATUS('MEDICORE_DB.RAW.PATIENTS_PIPE');

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'PATIENTS_RAW',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
));

-- ============================================================
-- SECTION 15: SCHEDULED TASKS
-- FIX: Snowflake TASK AS clause requires a single SQL statement.
--      Use a stored procedure wrapper for multi-step logic.
-- ============================================================

USE SCHEMA MEDICORE_DB.VALIDATED;

-- Master pipeline procedure (wraps all incremental steps)
CREATE OR REPLACE PROCEDURE MEDICORE_DB.VALIDATED.RUN_INCREMENTAL_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL MEDICORE_DB.VALIDATED.LOAD_PATIENTS_INCREMENTAL();
    CALL MEDICORE_DB.VALIDATED.LOAD_DOCTORS_INCREMENTAL();
    CALL MEDICORE_DB.CURATED.SCD2_PATIENTS();
    CALL MEDICORE_DB.CURATED.SCD2_DOCTORS();
    CALL MEDICORE_DB.VALIDATED.RUN_ALL_ANOMALIES();
    RETURN 'Incremental pipeline completed';
END;
$$;

-- Task: Incremental Pipeline (every 5 minutes)
--   FIX: AS clause is a single CALL statement, not BEGIN..END block
CREATE OR REPLACE TASK TASK_INCREMENTAL_PIPELINE
    WAREHOUSE = HOSPITAL_WH
    SCHEDULE  = '5 MINUTE'
AS
CALL MEDICORE_DB.VALIDATED.RUN_INCREMENTAL_PIPELINE();

ALTER TASK TASK_INCREMENTAL_PIPELINE RESUME;

-- Task: KPI Refresh (every 10 minutes)
--   FIX: KPI_SUMMARY table now exists (created in Section 11.7)
CREATE OR REPLACE TASK TASK_KPI_REFRESH
    WAREHOUSE = HOSPITAL_WH
    SCHEDULE  = '10 MINUTE'
AS
INSERT INTO MEDICORE_DB.CURATED.KPI_SUMMARY (summary_date, total_admissions)
SELECT CURRENT_DATE(), COUNT(*) FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;

ALTER TASK TASK_KPI_REFRESH RESUME;

-- ============================================================
-- SECTION 16: DATA MASKING POLICIES
-- ============================================================

USE ROLE SYSADMIN;
USE SCHEMA MEDICORE_DB.CURATED;

CREATE OR REPLACE MASKING POLICY mask_email
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN val
    ELSE 'hidden_email@medicore.com'
END;

CREATE OR REPLACE MASKING POLICY mask_phone
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN val
    ELSE 'XXXXXX'
END;

CREATE OR REPLACE MASKING POLICY mask_insurance
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN val
    ELSE 'INS-XXXXX'
END;

ALTER TABLE MEDICORE_DB.CURATED.DIM_PATIENTS
    MODIFY COLUMN email        SET MASKING POLICY mask_email;

ALTER TABLE MEDICORE_DB.CURATED.DIM_PATIENTS
    MODIFY COLUMN phone        SET MASKING POLICY mask_phone;

ALTER TABLE MEDICORE_DB.CURATED.DIM_PATIENTS
    MODIFY COLUMN insurance_id SET MASKING POLICY mask_insurance;

-- Test masking (as SYSADMIN = sees real values)
SELECT patient_id, email, phone, insurance_id
FROM MEDICORE_DB.CURATED.DIM_PATIENTS
LIMIT 10;

-- Test masking (as PUBLIC = sees masked values)
USE ROLE PUBLIC;
SELECT patient_id, email, phone, insurance_id
FROM MEDICORE_DB.CURATED.DIM_PATIENTS
LIMIT 10;

-- ============================================================
-- SECTION 17: ROW-LEVEL SECURITY (Department-Based)
-- FIX: Policy aligned to actual DEPARTMENT column values
--      (CARDIO, ORTHO, ER, IPD, OPD, etc.)
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE ROLE CARDIO_ROLE;
CREATE OR REPLACE ROLE ORTHO_ROLE;
CREATE OR REPLACE ROLE ER_ROLE;
CREATE OR REPLACE ROLE IPD_ROLE;
CREATE OR REPLACE ROLE OPD_ROLE;

USE ROLE ACCOUNTADMIN;

-- CARDIO ROLE
GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE CARDIO_ROLE;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE CARDIO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE CARDIO_ROLE;

-- ORTHO ROLE
GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE ORTHO_ROLE;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE ORTHO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ORTHO_ROLE;

-- ER ROLE
GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE ER_ROLE;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE ER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ER_ROLE;

-- IPD ROLE
GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE IPD_ROLE;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE IPD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE IPD_ROLE;

-- OPD ROLE
GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE OPD_ROLE;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE OPD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE OPD_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE CARDIO_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ORTHO_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE IPD_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE OPD_ROLE;

-- NOTE: Snowflake doesn't support FOR loops in DDL directly;
--       execute the GRANT statements individually:
GRANT USAGE  ON DATABASE MEDICORE_DB TO ROLE CARDIO_ROLE;
GRANT USAGE  ON SCHEMA MEDICORE_DB.CURATED TO ROLE CARDIO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE CARDIO_ROLE;

GRANT USAGE  ON DATABASE MEDICORE_DB TO ROLE ORTHO_ROLE;
GRANT USAGE  ON SCHEMA MEDICORE_DB.CURATED TO ROLE ORTHO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ORTHO_ROLE;

GRANT USAGE  ON DATABASE MEDICORE_DB TO ROLE ER_ROLE;
GRANT USAGE  ON SCHEMA MEDICORE_DB.CURATED TO ROLE ER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE ER_ROLE;

GRANT USAGE  ON DATABASE MEDICORE_DB TO ROLE IPD_ROLE;
GRANT USAGE  ON SCHEMA MEDICORE_DB.CURATED TO ROLE IPD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE IPD_ROLE;

GRANT USAGE  ON DATABASE MEDICORE_DB TO ROLE OPD_ROLE;
GRANT USAGE  ON SCHEMA MEDICORE_DB.CURATED TO ROLE OPD_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MEDICORE_DB.CURATED TO ROLE OPD_ROLE;

USE ROLE SYSADMIN;
GRANT ROLE CARDIO_ROLE TO USER SURAJ56;
GRANT ROLE ORTHO_ROLE  TO USER SURAJ56;
GRANT ROLE ER_ROLE     TO USER SURAJ56;
GRANT ROLE IPD_ROLE    TO USER SURAJ56;
GRANT ROLE OPD_ROLE    TO USER SURAJ56;

SHOW GRANTS TO USER SURAJ56;

-- Row Access Policy
--   FIX: Checks DEPARTMENT column (the actual physical department),
--        not admission_type (OPD/IPD/ER is a different field)
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE ROW ACCESS POLICY DEPT_RLS_POLICY
AS (department STRING)
RETURNS BOOLEAN ->
CASE
    WHEN CURRENT_ROLE() = 'SYSADMIN'    THEN TRUE
    WHEN CURRENT_ROLE() = 'CARDIO_ROLE' AND department = 'CARDIO' THEN TRUE
    WHEN CURRENT_ROLE() = 'ORTHO_ROLE'  AND department = 'ORTHO'  THEN TRUE
    WHEN CURRENT_ROLE() = 'ER_ROLE'     AND department = 'ER'     THEN TRUE
    WHEN CURRENT_ROLE() = 'IPD_ROLE'    AND department = 'IPD'    THEN TRUE
    WHEN CURRENT_ROLE() = 'OPD_ROLE'    AND department = 'OPD'    THEN TRUE
    ELSE FALSE
END;

SHOW ROW ACCESS POLICIES;

ALTER TABLE MEDICORE_DB.CURATED.FACT_ADMISSIONS
ADD ROW ACCESS POLICY DEPT_RLS_POLICY ON (department);

-- Test RLS
USE ROLE ER_ROLE;
SELECT DISTINCT department FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;

USE ROLE SYSADMIN;
SELECT DISTINCT department FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;
-- RLS Implementation → SUCCESS

-- ============================================================
-- SECTION 18: KPI QUERIES
-- ============================================================

USE ROLE SYSADMIN;
USE SCHEMA MEDICORE_DB.CURATED;

-- KPI 1 — Bed Occupancy & Utilization Score
--   Definition: Occupied bed hours / Total available bed hours
SELECT
    SUM(
        CASE
            WHEN DATEDIFF(MINUTE, admission_time, discharge_time) = 0 THEN 60
            ELSE DATEDIFF(MINUTE, admission_time, discharge_time)
        END
    ) / 60.0
    /
    COUNT(DISTINCT bed_no) AS bed_utilization_hours_per_bed
FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;

-- KPI 2 — Admission Turnaround Time (ATT)
--   Definition: Average (discharge_time - admission_time)
SELECT
    AVG(
        CASE
            WHEN DATEDIFF(MINUTE, admission_time, discharge_time) = 0 THEN 60
            ELSE DATEDIFF(MINUTE, admission_time, discharge_time)
        END
    ) / 60.0 AS avg_att_hours
FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;

-- KPI 3 — Surgery Efficiency Index
--   Definition: % of surgeries starting within scheduled_time ± 10 minutes
SELECT
    COUNT_IF(ABS(DATEDIFF(MINUTE, scheduled_time, start_time)) <= 10)
    / NULLIF(COUNT(*), 0) * 100 AS surgery_efficiency_pct
FROM MEDICORE_DB.CURATED.FACT_PROCEDURES;

-- KPI 4 — Doctor Workload Index
--   Definition: Avg admissions + procedures handled per doctor per day
SELECT
    d.doctor_id,
    d.name,
    COUNT(DISTINCT a.admission_id) AS total_admissions,
    COUNT(DISTINCT p.procedure_id) AS total_procedures,
    COUNT(DISTINCT a.admission_id) + COUNT(DISTINCT p.procedure_id) AS total_workload
FROM MEDICORE_DB.CURATED.DIM_DOCTORS d
LEFT JOIN MEDICORE_DB.CURATED.FACT_ADMISSIONS a
    ON d.doctor_sk = a.doctor_sk
LEFT JOIN MEDICORE_DB.CURATED.FACT_PROCEDURES p
    ON d.doctor_sk = p.doctor_sk
WHERE d.is_current = TRUE
GROUP BY d.doctor_id, d.name
ORDER BY total_workload DESC;

-- KPI 5 — Billing Accuracy Index
--   Definition: 1 - (# billing anomalies / total bills)
SELECT
    1 - (
        COUNT(*) / NULLIF((SELECT COUNT(*) FROM MEDICORE_DB.CURATED.FACT_BILLING), 0)
    ) AS billing_accuracy_index
FROM MEDICORE_DB.VALIDATED.ANOMALY_LOG
WHERE anomaly_type IN ('HIGH_BILLING', 'DUPLICATE_BILL_TIME');

-- ============================================================
-- SECTION 19: KPI DASHBOARD VIEWS (PUBLIC ACCESS)
-- ============================================================

USE ROLE SYSADMIN;
USE SCHEMA MEDICORE_DB.CURATED;

CREATE OR REPLACE VIEW KPI_DASHBOARD AS
SELECT
    /* KPI 1 — Bed Utilization */
    SUM(
        CASE
            WHEN DATEDIFF(MINUTE, admission_time, discharge_time) = 0 THEN 60
            ELSE DATEDIFF(MINUTE, admission_time, discharge_time)
        END
    ) / 60.0 / COUNT(DISTINCT bed_no) AS bed_utilization,

    /* KPI 2 — Average Treatment Time (hours) */
    AVG(
        CASE
            WHEN DATEDIFF(MINUTE, admission_time, discharge_time) = 0 THEN 60
            ELSE DATEDIFF(MINUTE, admission_time, discharge_time)
        END
    ) / 60.0 AS avg_att_hours

FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS;

CREATE OR REPLACE VIEW KPI_SURGERY_BILLING AS
SELECT
    /* KPI 3 — Surgery Efficiency */
    (
        SELECT
            COUNT_IF(ABS(DATEDIFF(MINUTE, scheduled_time, start_time)) <= 10)
            / NULLIF(COUNT(*), 0) * 100
        FROM MEDICORE_DB.CURATED.FACT_PROCEDURES
    ) AS surgery_efficiency_pct,

    /* KPI 5 — Billing Accuracy */
    (
        SELECT
            1 - (COUNT(*) / NULLIF((SELECT COUNT(*) FROM MEDICORE_DB.CURATED.FACT_BILLING), 0))
        FROM MEDICORE_DB.VALIDATED.ANOMALY_LOG
        WHERE anomaly_type IN ('HIGH_BILLING', 'DUPLICATE_BILL_TIME')
    ) AS billing_accuracy_index;

-- Grant PUBLIC access to KPI views
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE MEDICORE_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA MEDICORE_DB.CURATED TO ROLE PUBLIC;
GRANT SELECT ON VIEW MEDICORE_DB.CURATED.KPI_DASHBOARD        TO ROLE PUBLIC;
GRANT SELECT ON VIEW MEDICORE_DB.CURATED.KPI_SURGERY_BILLING  TO ROLE PUBLIC;

-- ============================================================
-- SECTION 20: FINAL VERIFICATION QUERIES
-- ============================================================

USE ROLE SYSADMIN;

-- Row counts across all layers
SELECT 'PATIENTS_RAW'        AS tbl, COUNT(*) AS cnt FROM MEDICORE_DB.RAW.PATIENTS_RAW
UNION ALL
SELECT 'DOCTORS_RAW',               COUNT(*)         FROM MEDICORE_DB.RAW.DOCTORS_RAW
UNION ALL
SELECT 'ADMISSIONS_RAW',            COUNT(*)         FROM MEDICORE_DB.RAW.ADMISSIONS_RAW
UNION ALL
SELECT 'PROCEDURES_RAW',            COUNT(*)         FROM MEDICORE_DB.RAW.PROCEDURES_RAW
UNION ALL
SELECT 'BILLING_RAW',               COUNT(*)         FROM MEDICORE_DB.RAW.BILLING_RAW
UNION ALL
SELECT 'PATIENTS_VALIDATED',        COUNT(*)         FROM MEDICORE_DB.VALIDATED.PATIENTS_VALIDATED
UNION ALL
SELECT 'DOCTORS_VALIDATED',         COUNT(*)         FROM MEDICORE_DB.VALIDATED.DOCTORS_VALIDATED
UNION ALL
SELECT 'ADMISSIONS_VALIDATED',      COUNT(*)         FROM MEDICORE_DB.VALIDATED.ADMISSIONS_VALIDATED
UNION ALL
SELECT 'PROCEDURES_VALIDATED',      COUNT(*)         FROM MEDICORE_DB.VALIDATED.PROCEDURES_VALIDATED
UNION ALL
SELECT 'BILLING_VALIDATED',         COUNT(*)         FROM MEDICORE_DB.VALIDATED.BILLING_VALIDATED
UNION ALL
SELECT 'DIM_PATIENTS',              COUNT(*)         FROM MEDICORE_DB.CURATED.DIM_PATIENTS
UNION ALL
SELECT 'DIM_DOCTORS',               COUNT(*)         FROM MEDICORE_DB.CURATED.DIM_DOCTORS
UNION ALL
SELECT 'FACT_ADMISSIONS',           COUNT(*)         FROM MEDICORE_DB.CURATED.FACT_ADMISSIONS
UNION ALL
SELECT 'FACT_PROCEDURES',           COUNT(*)         FROM MEDICORE_DB.CURATED.FACT_PROCEDURES
UNION ALL
SELECT 'FACT_BILLING',              COUNT(*)         FROM MEDICORE_DB.CURATED.FACT_BILLING
ORDER BY tbl;

-- Anomaly summary
SELECT anomaly_type, COUNT(*) AS hits
FROM MEDICORE_DB.VALIDATED.ANOMALY_LOG
GROUP BY anomaly_type
ORDER BY hits DESC;

-- DQ failures summary
SELECT issue_type, COUNT(*) AS failures
FROM MEDICORE_DB.VALIDATED.DQ_FAILURE_LOG
GROUP BY issue_type
ORDER BY failures DESC;

-- Load audit
SELECT * FROM MEDICORE_DB.VALIDATED.LOAD_AUDIT_LOG ORDER BY load_time DESC;

-- KPI Dashboard
SELECT * FROM MEDICORE_DB.CURATED.KPI_DASHBOARD;
SELECT * FROM MEDICORE_DB.CURATED.KPI_SURGERY_BILLING;


SELECT * FROM MEDICORE_DB.CURATED.KPI_DASHBOARD;
SELECT * FROM MEDICORE_DB.CURATED.KPI_SURGERY_BILLING;
