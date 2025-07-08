# Retail Analytics with PySpark

This project demonstrates how to build a scalable retail analytics pipeline using Apache Spark (PySpark). It simulates a real-world environment involving customer profiles and transaction data, and includes:

- Batch data ingestion
- Data transformation and aggregation
- SQL-based analytics and UDFs
- Machine learning with Spark MLlib for churn prediction

## Project Structure

```
pyspark-retail-analytics/
│
├── data/
│   ├── customers.csv              # Synthetic customer dataset
│   └── transactions.csv           # Synthetic retail transactions
│
├── scripts/
│   ├── generate_customers.py      # Generates customers.csv using Faker
│   ├── generate_transactions.py   # Generates transactions.csv linked to customers
│   ├── data_ingestion.py          # Loads and explores CSV data with PySpark
│   ├── data_clean_transform.py    # Cleans and transforms customer dataset
│   ├── join_analytics.py          # Joins customer and transaction data, aggregates spend
│   ├── sql_analytics.py           # Performs SQL queries and UDF-based analysis
│   └── churn_prediction.py        # ML model to predict customer churn
```

## Features Implemented

### Data Ingestion
- Load structured CSV data using `spark.read.csv`
- Schema inference and exploration
- Convert between DataFrame and RDD

### Data Cleaning & Transformation
- Handle missing values and type casting
- Add derived columns such as `is_churned`
- Filter and group data using `withColumn`, `when`, `groupBy`

### Join & Aggregation
- Inner join between customer and transaction datasets
- Compute customer-level metrics such as `total_spent` and `transaction_count`
- Use aggregation functions (`sum`, `count`) and sorting

### SQL & UDFs
- Register Spark DataFrames as SQL temporary views
- Query data using SQL (`SELECT`, `GROUP BY`, `ORDER BY`, `LIMIT`)
- Define and use UDFs (user-defined functions) for custom logic

### Machine Learning (MLlib)
- Feature engineering using `StringIndexer`, `VectorAssembler`
- Train a `LogisticRegression` model to predict churn
- Evaluate model with `BinaryClassificationEvaluator` (AUC metric)
```
     ┌─────────────────────┐
     │   Raw Data Sources  │
     │  • customers.csv    │
     │  • transactions.csv │
     │  • inventory.csv    │
     │  • streaming_orders │
     └──────────┬──────────┘
                │
                ▼
     ┌─────────────────────┐
     │  Module 1: Ingestion│
     │  • SparkSession     │
     │  • Schema Definition│
     │  • Data Loading     │
     │  • Sample Generation│
     └──────────┬──────────┘
                │
                ▼
     ┌─────────────────────┐
     │ Module 2: Cleaning  │
     │  • Quality Checks   │
     │  • Feature Engineer │
     │  • Joins & Aggregs  │
     │  • Window Functions │
     └──────────┬──────────┘
                │
                ▼
     ┌─────────────────────┐
     │  Optimized Storage  │
     │  • Parquet Format   │
     │  • Data Partitioning│
     │  • Memory Caching   │
     └─┬─────────────────┬─┘
       │                 │
       ▼                 ▼
┌──────────────┐ ┌──────────────┐ 
│ Module 3:    │ │ Module 4:    │ 
│ SQL Analytics│ │ Machine      │ 
│ • BI Reports │ │ Learning     │ 
│ • Customer   │ │ • Churn      │ 
│   Segments   │ │   Prediction │ 
│ • KPI Dashbd │ │ • ML Pipeline│ 
└──────┬───────┘ └──────┬───────┘ 
       │                │                
       └───────┼────────┘
               │
               ▼
    ┌──────────────────────┐
    │ Business Intelligence│
    │ • Churn Predictions  │
    └──────────────────────┘
```              
## Requirements

- Python 3.8+
- Java 17 (Temurin recommended)
- PySpark 3.2 or higher
- pandas
- faker

Install dependencies:

```
pip install pyspark pandas faker
```

## How to Run

### 1. Generate synthetic data
```
python scripts/generate_customers.py
python scripts/generate_transactions.py
```

### 2. Ingest and clean data
```
python scripts/data_ingestion.py
python scripts/data_clean_transform.py
```

### 3. Analyze and aggregate
```
python scripts/join_analytics.py
python scripts/sql_analytics.py
```

### 4. Train machine learning model
```
python scripts/churn_prediction.py
```

## Output Examples

- Top 10 spending customers with churn label
- Customer transaction summaries grouped by gender and status
- Spender categories (High, Medium, Low)
- Model predictions and probabilities for churn
- AUC evaluation score from the logistic regression model

## Concepts Covered

- PySpark DataFrame API
- Lazy evaluation and execution DAGs
- Data transformation pipelines
- SQL operations and UDFs on Spark DataFrames
- Machine learning with Spark MLlib

## Notes

- This project runs in a local Spark session and is suitable for personal machines
- It can be extended to run on a Spark cluster or cloud platforms such as AWS EMR, Databricks, or GCP Dataproc
- Ideal for practice, learning, or inclusion in a data engineering portfolio
