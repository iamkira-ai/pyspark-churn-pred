## Overview

You've created a full PySpark mini-project with:

| Script                              | Purpose                                                   |
| ----------------------------------- | --------------------------------------------------------- |
| `1_data_ingestion.py`               | Load CSV data using Spark and explore DataFrame vs RDD    |
| `2_data_cleaning_transformation.py` | Clean, filter, and transform data using column operations |
| `3_join_analytics.py`               | Join two datasets and perform grouped aggregations        |
| `4_sql_analytics.py`                | Use SQL queries and UDFs on Spark tables                  |
| `5_churn_prediction.py`             | Machine learning using MLlib to predict customer churn    |

---

## 1. `data_ingestion.py`

### What it does:

* Starts a SparkSession
* Reads `customers.csv` into a DataFrame
* Converts it to an RDD and back
* Counts and prints data

### How it works (backend):

* Spark **lazily loads** the data: nothing runs until `.show()` or `.count()` is called
* The CSV file is parsed using **distributed file readers**
* Spark converts it into a **distributed DataFrame** spread across partitions
* `.rdd` gives you a **Resilient Distributed Dataset**: low-level parallel data structure used internally by Spark

### Compared to Pandas:

| Pandas                      | PySpark                                             |
| --------------------------- | --------------------------------------------------- |
| Data loaded in memory       | Data is split into distributed chunks               |
| Operations happen instantly | Operations are **lazy**, optimized before execution |
| Single machine              | Designed for **cluster and parallel processing**    |
| No concept of RDD           | RDD is core to Spark’s low-level execution          |

---

## 2. `data_cleaning_transformation.py`

### What it does:

* Cleans and casts columns (e.g., date)
* Adds computed columns (`is_churned`)
* Filters and groups data
* Renames columns

### How it works:

* All transformations are **lazy** and encoded into a DAG (Directed Acyclic Graph)
* When `.show()` or `.count()` is called, Spark:

  1. Optimizes the DAG via **Catalyst optimizer**
  2. Compiles it into JVM bytecode
  3. Executes across multiple worker nodes

### Compared to Pandas:

| Pandas                            | PySpark                                            |
| --------------------------------- | -------------------------------------------------- |
| Uses Python functions (`df['x']`) | Uses column expressions (`col("x")`, `withColumn`) |
| Memory-intensive                  | Distributed and scalable                           |
| No DAG or optimizer               | Catalyst optimizer + Tungsten execution engine     |

---

## 3. `join_analytics.py`

### What it does:

* Joins customers and transactions
* Calculates total spending and transaction count per customer
* Aggregates and sorts results

### How it works:

* Joins are **shuffle-heavy** operations
* Spark:

  1. Partitions both tables
  2. Reorganizes (shuffles) based on join keys
  3. Merges matching records

### Compared to Pandas:

| Pandas                       | PySpark                                                         |
| ---------------------------- | --------------------------------------------------------------- |
| `pd.merge()` joins in memory | Spark joins can be **broadcasted** or **shuffled across nodes** |
| CPU-bound                    | Can scale out across a cluster                                  |
| No need to tune partitions   | Spark allows repartitioning for performance                     |

---

## 4. `sql_analytics.py`

### What it does:

* Registers joined data as a SQL table
* Runs SQL queries (select, group, order)
* Adds a custom UDF to tag spender categories

### How it works:

* `createOrReplaceTempView()` stores DataFrame as a **virtual SQL table**
* `spark.sql()` compiles your SQL into Spark's execution plan
* UDFs let you apply **custom Python logic** — but they are slower because:

  * They break the optimization pipeline
  * They execute row-by-row in Python (not JVM)

### Compared to Pandas:

| SQL/Pandas                      | PySpark SQL                                        |
| ------------------------------- | -------------------------------------------------- |
| Pandas lacks built-in SQL       | Spark supports full ANSI SQL                       |
| `.query()` in Pandas is limited | `.sql()` supports joins, windows, subqueries, UDFs |
| Python functions work naturally | Spark UDFs require wrapping with types             |

---

## 5. `churn_prediction.py`

### What it does:

* Engineers features (`total_spent`, `transaction_count`, `gender_index`)
* Converts gender to numeric (StringIndexer)
* Uses VectorAssembler to create ML-ready vectors
* Trains a **Logistic Regression** model with MLlib
* Evaluates it using `BinaryClassificationEvaluator`

### How it works:

* MLlib uses **pipelined transformers and estimators** (just like Scikit-Learn)
* Spark builds a distributed pipeline that:

  1. Transforms features in stages
  2. Trains models using optimized distributed algorithms
  3. Applies models across partitions of test data

### Compared to Scikit-Learn:

| Scikit-Learn                | Spark MLlib                             |
| --------------------------- | --------------------------------------- |
| Works on `NumPy` / `Pandas` | Works on Spark DataFrames               |
| Not distributed             | Distributed training and prediction     |
| `fit()` works on small data | `fit()` on TB-scale data possible       |
| Everything in RAM           | Spark can spill and cache intelligently |

---

## Summary: Why PySpark over Pandas

| Feature             | Pandas / Scikit-Learn | PySpark (DataFrame + MLlib)         |
| ------------------- | --------------------- | ----------------------------------- |
| Data size           | Small/medium          | Medium to large / big data          |
| Speed               | Good (in-memory)      | Optimized + parallelized            |
| Cluster support     | No                    | Yes (built for distributed systems) |
| Lazy evaluation     | No                    | Yes                                 |
| DAG optimization    | No                    | Yes (Catalyst, Tungsten)            |
| ML pipeline support | Partial               | Full with built-in transformations  |
| SQL integration     | No native support     | Yes (ANSI-compliant SQL engine)     |