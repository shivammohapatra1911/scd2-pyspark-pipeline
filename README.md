# SCD Type 2 PySpark Pipeline

## Overview
This project implements Slowly Changing Dimension Type 2 (SCD2) using PySpark.

It tracks historical changes in customer data by:
- Expiring old records
- Inserting new records
- Maintaining active/inactive flags

## Features
- Change detection using joins
- SCD Type 2 logic
- Handles new customers
- Latest active record extraction using window function

## Tech Stack
- Python
- PySpark
- Spark (local setup)

## How to Run
1. Activate virtual environment
2. Run:
   python scd2_pipeline.py

## Example Output
Shows:
- Full SCD2 table
- Latest active records
