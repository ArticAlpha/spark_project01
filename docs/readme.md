# ETL Pipeline Project Using Apache Airflow and Spark

## Project Overview

This project demonstrates the implementation of an ETL 
(Extract, Transform, Load) pipeline using Apache Airflow, 
Apache Spark, and other essential tools in a containerized environment. 
The pipeline is designed to process and transform data systematically, 
with orchestration managed by Airflow, and distributed data processing handled by Spark.

## The pipeline follows these stages:

**Extract:** Downloading raw files from AWS S3.

**Transform:** Cleaning, transforming, and modeling the data using Spark jobs.

**Load:** Uploading the processed data back to AWS S3.

# **Architecture**

### Orchestrator: 
Apache Airflow handles scheduling, task management, and overall pipeline orchestration.

### Distributed Processing: 

Apache Spark is used for data cleaning, transformation, and modeling.
Database: MySQL is used for task-related metadata and other project needs.
Storage: AWS S3 is the data source and destination for the pipeline.
Containerization: Docker ensures consistent environments across the project components.

# Pipeline Workflow

### Below are the detailed steps of the ETL pipeline:

**Download File from S3 Script:** aws_file_download.py Task to download raw data files from an S3 bucket and store them locally for processing.

**Data Cleaning Script:** data_cleaning.py Cleanses the data by removing duplicates, handling null values, and applying standard formatting.

**Data Transformation Script:** transformation.py Applies transformations to prepare the data for downstream modeling.

## Dimension Modeling

**Script: dimension.py** Processes dimension data for star-schema data modeling.

**Script: custom_dimensions.py** Handles any custom transformations specific to dimension tables.

**Fact Modeling Script:** fact.py Processes fact data for star-schema data modeling.

**Upload Processed Data to S3 Script:** upload.py Uploads the cleaned and transformed data back to a specified S3 bucket.

# Prerequisites

Docker and Docker Compose installed on your machine. 

AWS S3 bucket access (with credentials).

MySQL setup for metadata storage.

## Python dependencies:
* boto3
* cryptography
* loguru
* pyspark
* findspark
* mysql-connector-python



