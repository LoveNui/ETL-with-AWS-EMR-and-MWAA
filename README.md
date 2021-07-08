# Batch-ETL-Using-AWS-EMR-in-Managed-Airflow

## Motivation

Data aggregation can be time consuming and can use up large amount of computing resources; this is especially the case when working with large dataset. Having to manually unpdate this process each time new data is introduced requires immediate attention before a data worker can move on to other tasks such as data analysis and model training. Automating such a process with a reusable data pipeline that runs regularly is critical in boosting team performance. Data scientists or data analysts then may be freed up from cleaning the data, directly using the output data from this pipeline for data analysis or for training Machine Learning model. 

## Introduction

**Datasets**: The source data consists of three CSV files and are stored on Cloud in an AWS S3 bucket. Each observation represents an individual's job posting; each column represents unqiue information about this applicant and the job applied to.
1. train_features.csv
2. test_features.csv
3. train_salaries.csv

**Goal**: Create a data pipeline using Amazon Managed Apache Airflow (MWAA) to orchestrate and automate batch ETL processing workflow in AWS. 

**Architecture Overivew**: At a high level, the AWS cloud environment is illustrated below.

![](images/architecture_overview.png)
