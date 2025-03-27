# IPL Matches Data Pipeline

## Overview
This project extracts **IPL matches data** from an **AWS S3 bucket**, processes and transforms it using **Databricks (PySpark)**, loads the transformed data into **Snowflake**, and finally connects Snowflake to **Power BI** for analysis. The goal is to analyze IPL matches year-wise.
![Screenshot 2025-03-22 120303](https://github.com/user-attachments/assets/ef9c526c-7ec9-451a-ad5c-2f37a72e05de)


## Workflow
1. **Extract:** Fetch IPL matches data from an **S3 bucket**.
2. **Transform:** Perform necessary transformations using **Databricks (PySpark)**.
3. **Load:** Insert processed data into a **Snowflake** table.
4. **Visualize:** Connect Snowflake to **Power BI** for in-depth analysis.

## Tech Stack Used
- **Amazon S3** - Data Storage
- **Databricks (PySpark)** - Data Processing & Transformation
- **Snowflake** - Data Warehouse
- **Power BI** - Data Visualization

## Steps Implemented
### 1. Data Extraction from S3 - I Used S3 bucket from Darshil Parmar IPL Data Analysis project
Used Databricks to read IPL match data stored in an S3 bucket.
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPLDataProcessing").getOrCreate()

df = spark.read.option("header", "true").csv("s3://your-bucket-name/ipl_matches.csv")
df.show(5)
```

### 2. Data Transformation in Databricks
Performed data cleaning and transformations like:
- Extracting year from match date
- Aggregating matches played per year
- Count the points tab;e by year


### 3. Loading Data into Snowflake
Configured Snowflake connection and loaded the transformed data.
```python
snowflake_options = {
    "sfURL": "https://your_snowflake_account.snowflakecomputing.com",
    "sfDatabase": "IPL_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "your_username",
    "sfPassword": "your_password"
}

df_grouped.write.format("snowflake").options(**snowflake_options).option("dbtable", "IPL_MATCHES_YEARLY").mode("overwrite").save()
```

### 4. Power BI Analysis
- Connected **Snowflake** to **Power BI** using the Snowflake connector.
- Created a **Yearly IPL Matches Analysis** dashboard.
- Visualized trends in the number of matches per year.

## Results
- Found insights on **how the number of IPL matches changed over the years**.
- Created **interactive Power BI reports**.

## Future Improvements
- Add more IPL statistics like team performance, player analysis, etc.
- Automate the **ETL pipeline** using **Airflow**.
- Implement **real-time data processing** using Kafka & Spark Streaming.

## How to Run This Project
1. Set up an **AWS S3 bucket** and upload IPL matches data.
2. Configure **Databricks** and run the PySpark scripts.
3. Load the transformed data into **Snowflake**.
4. Connect **Snowflake** to **Power BI** and create dashboards.

## Contributing
If you want to contribute, feel free to raise a PR or an issue. 😊

## Author
Badari Maddula - https://www.linkedin.com/in/badariphaniteja/

---
Let me know if you need any modifications! 🚀

