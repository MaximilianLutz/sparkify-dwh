# Data Warehouse Project - Sparkify

## Introduction
Sparkify, a music streaming startup, seeks to migrate their data and processes to the cloud. Their data, stored in Amazon S3, consists of JSON logs on user activity and song metadata. As the data engineer, our task is to build an ETL pipeline, extracting data from S3, staging it in Amazon Redshift, and transforming it into dimensional tables for analytics.

## Project Overview
This project involves:
- Extracting data from Amazon S3.
- Staging data in Amazon Redshift.
- Transforming staged data into dimensional tables.
- Optional: Running some analytics queries

## System Architecture
![System Architecture](additional_material/songify_schema.png)

## Project Description
You'll apply your data warehousing and AWS knowledge to create an ETL pipeline for a Redshift database. Key tasks include:
- Loading data from Amazon S3 into staging tables in Amazon Redshift.
- Executing SQL statements to create analytics tables from staged data.
