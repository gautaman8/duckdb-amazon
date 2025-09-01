# DuckDB Amazon Data Analysis

Simple data analysis pipeline using DuckDB to analyze Amazon UK product data.

## Setup

### 1. Create Virtual Environment
```bash
python3 -m venv env
source env/bin/activate  # On macOS/Linux

```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

## Usage

### Run Analysis Pipeline
```bash
python db.py
```

## Completed Features

- Grouped by product category
    - Avg rating
    - Variance of rating
    - Standard Deviation of rating
- Categories with 
    - highest variability (TOP 5)
    - lowest variability (BOTTOM 5)
- Categories with
    - high z-score (Top 5)
    - low z-score (Bottom 5)


Z score shows how much category specific average has deviated from global average

## Future steps 

### App development

- If data is ingested as a csv, fresh analysis stats are required only when old data has changes
- Trigger analytical pipeline when there's a refresh of new data available
- Save the results of pipeline in a seperate parquet file/duckdb file in Cloud Storage/S3 buckets
- Use FastAPI backend service to retrieve the stored stats from Cloud Storage
- Frontend can connect with the FastAPI service to display the stats in a webapp

### Scalability Considerations
- If data volume is too high, data can be sharded and stats can be consolidated
- I've not explored this area much, but most databases have provisions to set this up



