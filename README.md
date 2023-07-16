# Data Pipeline with Apache Airflow

## Project overview
This repository is aimed to load JSON files from AWS S3 bucket to AWS Redshift cluster using Apache Airflow with the main emphasis on creating custom Airflow operators.

## Assignment
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

It is expected that high grade data pipelines are created, and they are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Sparkify team have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Overview of the files in the repository
- create_tables.sql file contains SQL queries to create table in Redshift cluster
- 'dags' folder contains dag.py file
- 'plugins' folder contains 'helpers' and 'operators' folders that has additional SQL queries and custom operators respectively
- 'img' folder contains images for the current file

# Running the project

## Pre-requisites
- In your AWS account:
    - prepare S3 bucket with the dataset (you can use this[http://millionsongdataset.com/] dataset or it's subset)
    - prepare Redshift cluster for the output (note that it should be in the same region as S3 bucket)
    - prepare an IAM User in AWS that can communicate with both S3 bucket (read permissions) and Redshift cluster (full permissions)
    - install Apache Airflow and add your User and Cluster details as Connections

## How to run the project
Run dag.py file in the terminal. You can manually monitor the progress in Airflow UI.
