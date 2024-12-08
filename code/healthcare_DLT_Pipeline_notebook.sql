-- Databricks notebook source
-- Databricks notebook source
CREATE LIVE TABLE diagnostic_mapping
COMMENT "Bronze table for the diagnosis mapping file"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM healthcare.raw_diagnosis_mapping

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE daily_patients
COMMENT "Bronze table for daily patient data"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM STREAM(healthcare.raw_patients_daily)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE processed_patient_data (CONSTRAINT valid_data EXPECT (patient_id IS NOT NULL and `name` IS NOT NULL and age IS NOT NULL and gender IS NOT NULL and `address` IS NOT NULL and contact_number IS NOT NULL and admission_date IS NOT NULL) ON VIOLATION DROP ROW)
COMMENT "Silver table with newly joined data from bronze tables and data quality constraints" 
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
p.patient_id,
p.name,
p.age,
p.gender,
p.address,
p.contact_number,
p.admission_date,
m.diagnosis_description
FROM STREAM (live.daily_patients) p
LEFT JOIN live.diagnostic_mapping m
on p.diagnosis_code = m.diagnosis_code;

-- COMMAND ----------

CREATE LIVE TABLE patient_statistics_by_diagnosis
COMMENT "Gold table with detailed patient statistics by diagnosis description"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
  diagnosis_description,
  count(patient_id) as patient_count,
  avg(age) as avg_age,
  count(distinct gender) as unique_gender_count,
  min(age) as min_age,
  max(age) as max_age
FROM live.processed_patient_data
group by diagnosis_description;

-- COMMAND ----------

CREATE LIVE TABLE patient_statistics_by_gender
COMMENT "Gold table with detailed patient statistics by gender"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  gender,
  count(patient_id) as patient_count,
  avg(age) as avg_age,
  count(distinct diagnosis_description) as unique_diagnosis_count,
  min(age) as min_age,
  max(age) as max_age
FROM live.processed_patient_data
GROUP BY gender;