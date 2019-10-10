# **Data Lake with Spark and AWS S3**
## **Project Summary**
Mock music app Company, Sparkify's user base and song database has grown significantly and therefore want to move their data warehouse to data lake. Goal of the project is to build an ETL pipeline using PySpark to extract Company's JSON formatted data files from AWS S3 and load them back onto S3 as a set of dimensional tables after required transformations. Key requirement of the project is to optimize the dimensional tables for songplay analysis.

## **Project Datasets**
### **Songs Dataset**
Subset of the original dataset, Million Song Dataset (Columbia University). Stored on S3 in JSON format containing song metadata. Each file contains metadata for a single song. The files are partitioned by the first three letters of each song's track ID.

### **Log (Events) Dataset**
Dataset is generated using Eventsim simulator hosted on Github and saved on S3 for the project. Files are JSON formatted and each file contains user activity for a single day. The log files in the dataset are partitioned by the year and month.

## **Database Design**
### **Star Schema**
Database is designed in a Star schema format with one Fact table, Songsplay and four Dimension tables, users, songs, artists, and time.

## **ETL Process**
ETL tasks are processed using pyspark.sql module from Python API for Spark, PySpark. Primarily using Spark DataFrame functionality to read, transform, and write Songs and Log data. To read the data from AWS S3, user's AWS credentials are supplied in separate config file, parsed during the script runtime. Upon sucessful access to S3, data is recurcively read into Spark DataFrame using JSON read method from the given path. Various transformations are performed to create multiple dimensional tables from specific DataFrames. Dimensional tables are stored back onto S3 in Parquet file format with some tables logically partitioned by specific columns to allow optimization on read for analysis. 

## **Python Script & Deployment**
Python script, 'etl.py' reads and sets user's AWS credentials, defines functions that instantiates SparkSession, reads and processes Songs data from S3, reads and processes Log data from S3, and writes transformed dimensional tables to S3.

Multiple Python libraries are imported to enable various Spark functionalities, datetime processing, and logging errors.

Script expects config file 'dl.cfg' containing AWS credentials stored on the same path as script. If the config file is missing or not formatted properly, script causes error and exits without running further steps.

In addition to config file, a Hadoop JAR must be supplied when deploying the spark-submit application. 

Run PySpark script with YARN cluster manager using below command from CLI connected to AWS EMR:
spark-submit --master yarn --packages 'org.apache.hadoop:hadoop-aws:2.8.5' etl.py
