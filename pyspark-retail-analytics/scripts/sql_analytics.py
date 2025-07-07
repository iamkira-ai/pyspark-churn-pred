import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SparkSQL-UDF").getOrCreate()

# Load data
customers = spark.read.option("header", True).csv("../data/customers.csv")
transactions = spark.read.option("header", True).csv("../data/transactions.csv")

# Cast fields
transactions = transactions.withColumn("quantity", col("quantity").cast("int")) \
                           .withColumn("price", col("price").cast("float")) \
                           .withColumn("total_spent", col("quantity") * col("price"))

# Join
joined = transactions.join(customers, on="customer_id", how="inner")

# Register as SQL tables
joined.createOrReplaceTempView("customer_transactions")

# SQL Query: total spent per customer
sql_query = """
SELECT 
    customer_id, 
    name,
    gender,
    status,
    ROUND(SUM(total_spent), 2) AS total_spent,
    COUNT(*) AS transactions
FROM customer_transactions
GROUP BY customer_id, name, gender, status
ORDER BY total_spent DESC
LIMIT 10
"""

top_customers = spark.sql(sql_query)
top_customers.show()

# ------------------------
# Define a Python function + UDF
def label_spender(amount):
    if amount > 3000:
        return "High"
    elif amount > 1000:
        return "Medium"
    else:
        return "Low"

label_udf = udf(label_spender, StringType())

# Use the UDF
labeled_customers = top_customers.withColumn("spender_category", label_udf(col("total_spent")))
labeled_customers.show()

spark.stop()
