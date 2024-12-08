# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/healthcare/raw_data/')

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/healthcare/raw_data/diagnosis_mapping.csv")
display(df)

df.write.format("delta").mode("append").saveAsTable("pyspark_playground.healthcare.raw_diagnosis_mapping")

# COMMAND ----------

path1 = "dbfs:/FileStore/healthcare/raw_data/patients_daily_file_1_2024.csv"
path2 = "dbfs:/FileStore/healthcare/raw_data/patients_daily_file_2_2024.csv"
path3 = "dbfs:/FileStore/healthcare/raw_data/patients_daily_file_3_2024.csv"

df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(path3)

df1 = df1.withColumn("admission_date",df1["admission_date"].cast("date"))
display(df1)

df1.write.format("delta").option("mergeSchema","True").mode("append").saveAsTable("pyspark_playground.healthcare.raw_patients_daily")

# COMMAND ----------

# %sql 
# TRUNCATE TABLE pyspark_playground.healthcare.raw_diagnosis_mapping;
# DROP TABLE pyspark_playground.healthcare.raw_diagnosis_mapping;

# TRUNCATE TABLE pyspark_playground.healthcare.raw_patients_daily;
# DROP TABLE pyspark_playground.healthcare.raw_patients_daily;