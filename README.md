# Healthcare DLT Pipeline  

This repository contains a **Healthcare DLT Pipeline** implemented using Databricks Delta Live Tables (DLT). The project processes healthcare data by ingesting diagnosis mapping and daily patient data, transforming it through raw, bronze, silver, and gold layers, and applying data quality constraints and aggregations to generate insights.  

## Features  
- **Data Sources**:  
  - `mapping_data`: Contains diagnosis names and IDs.  
  - `daily_patients`: Daily patient data with admission details.  

- **Delta Table Layers**:  
  - **Raw Tables**: Initial ingestion of data from GCP Buckets.  
  - **Bronze Tables**: Basic cleaning and schema alignment.  
  - **Silver Tables**: Streaming table joins and data quality enforcement.  
  - **Gold Tables**: Aggregated insights such as patient statistics by diagnosis and gender.  

- **Orchestration**: End-to-end pipeline built using Databricks DLT for seamless data processing.  

## Data Flow  
1. **Ingestion**:  
   Data files are uploaded to GCP Buckets and read into Delta tables on Databricks (raw layer).  

2. **Bronze Tables**:  
   - `diagnostic_mapping`: Created from `mapping_data`.  
   - `daily_patients`: Streaming table from `daily_patients` data.  

3. **Silver Tables**:  
   - `processed_patient_data`: Joins `diagnostic_mapping` and `daily_patients` with quality constraints.  

4. **Gold Tables**:  
   - `patient_statistics_by_diagnosis`: Aggregates patient counts, average age, etc., by diagnosis.  
   - `patient_statistics_by_gender`: Aggregates statistics by gender.  

## Repository Structure  


## Prerequisites  
- Databricks workspace with DLT enabled.  
- GCP Bucket access for data storage.  
- Delta Lake library installed.  

## How to Run  
1. Upload data files (`diagnosis_mapping.csv` and `patients_daily_file_*.csv`) to GCP Buckets.  
2. Configure paths in the Databricks notebooks.  
3. Execute the `raw_data_ingestion.ipynb` to load data into raw Delta tables.  
4. Run the `healthcare_dlt_pipeline.ipynb` to create and orchestrate DLT tables.  

## Output  
- **Bronze Layer**: Cleaned data.  
- **Silver Layer**: Processed patient data with quality constraints.  
- **Gold Layer**: Aggregated insights for analysis.  

## Contributions  
Contributions are welcome! If you find any issues or want to add new features, feel free to fork the repository, make your changes, and submit a pull request.  

## License  
This project is licensed under the [MIT License](LICENSE).  

## Acknowledgements  
- **Databricks Documentation** for guidance on Delta Live Tables.  
- **Delta Lake** for providing a robust storage and processing framework.  
- **Google Cloud Platform (GCP)** for seamless cloud storage and integration.  
