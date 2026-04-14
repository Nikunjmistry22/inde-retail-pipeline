# Retail ETL Pipeline

## Overview

This project implements an end-to-end ETL pipeline to ingest, process, validate, and standardize retail sales data from multiple B2B vendors with inconsistent schemas and formats.

The pipeline ensures that raw data from different sources is transformed into a unified structure suitable for downstream analytics.

---

## Problem Statement

Vendor data is received in heterogeneous formats with:

* Different schemas and column names
* Inconsistent date formats
* Missing or malformed values

The objective is to standardize this data into a single, clean dataset.

---

## Final Output Schema

| Column Name        | Description                            |
| ------------------ | -------------------------------------- |
| date               | Transaction date                       |
| product_identifier | Unique SKU identifier                  |
| total_units        | Total units sold                       |
| total_revenue      | Total revenue generated                |
| data_source        | Source platform (Blinkit, Zepto, etc.) |

---

## Project Structure

```
retail-etl-pipeline/
│
├── data/                  # Input data files
├── output/                # Final processed dataset
├── logs/                  # Execution logs
│
├── src/
│   ├── ingest.py         # Source-specific ingestion logic
│   ├── pipeline.py       # Main pipeline orchestration
│   ├── validation.py     # Data quality checks
│   └── utils.py          # Helper functions and logging setup
│
├── requirements.txt
├── README.md
└── .github/workflows/
    └── pipeline.yml      # CI pipeline
```

---

## Pipeline Design

### 1. Ingestion

* Automatically scans the `data/` directory
* Identifies files based on source name (Blinkit, Zepto, Nykaa, Myntra)
* Applies source-specific parsing logic

### 2. Processing & Standardization

Each source is transformed into a common schema:

* Column mapping and renaming
* Date normalization
* Numeric cleaning and type conversion
* Revenue calculation logic per source

### 3. Aggregation

Data is aggregated at:

* date × product_identifier × data_source

### 4. Validation

The pipeline performs:

* Schema validation
* Null checks
* Data integrity checks (negative revenue, zero units)

### 5. Output

Final dataset is saved as:

```
output/final_sales.csv
```

---

## Logging

* Logs are generated for each pipeline run
* Stored in the `logs/` directory
* Include:

  * File processing status
  * Row counts
  * Errors and warnings
  * Validation results

---

## How to Run

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Run pipeline

```
python src/pipeline.py
```

---

## CI/CD

A GitHub Actions workflow is configured to:

* Run the pipeline on every push
* Validate execution
* Upload logs and output as artifacts

---

## Assumptions

* File names contain the source identifier (e.g., "blinkit", "zepto")
* Input data is placed inside the `data/` directory
* Required columns are present in each source file
* Revenue definitions differ by source and are handled accordingly

---


