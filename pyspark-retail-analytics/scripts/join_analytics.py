import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

spark = SparkSession.builder \
    .appName("CustomerTransactions") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

customers = spark.read.option("header", True).csv("../data/customers.csv")
transactions = spark.read.option("header", True).csv("../data/transactions.csv")

# Cast types
transactions = transactions.withColumn("quantity", col("quantity").cast("int")) \
                           .withColumn("price", col("price").cast("float"))

# Join on customer_id
joined = transactions.join(customers, on="customer_id", how="inner")

# Calculate total spend per customer
customer_spending = joined.withColumn("total_spent", col("quantity") * col("price")) \
                          .groupBy("customer_id", "name", "gender", "status") \
                          .agg(
                              _sum("total_spent").alias("total_spent"),
                              count("transaction_id").alias("transaction_count")
                          )

# Show results
print("Customer Spending Summary:")
customer_spending.orderBy(col("total_spent").desc()).show(10)

# Optional: Gender-based spend average
gender_spending = customer_spending.groupBy("gender").agg(_sum("total_spent").alias("total_gender_spend"))
print("Spend by Gender:")
gender_spending.show()

spark.stop()
