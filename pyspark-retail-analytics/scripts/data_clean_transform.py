import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit


# Start SparkSession
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load customer data
df = spark.read.option("header", True).csv("../data/customers.csv")

print("Original Data:")
df.show(5)

# 1. Cast signup_date to actual date type
df = df.withColumn("signup_date", col("signup_date").cast("date"))

# 2. Add a new column: is_churned (1 if churned, else 0)
df = df.withColumn("is_churned", when(col("status") == "churned", lit(1)).otherwise(lit(0)))

# 3. Filter: Only female customers who are active
female_active = df.filter((col("gender") == "Female") & (col("status") == "active"))

# 4. Group by gender and status → count
summary = df.groupBy("gender", "status").count()

# 5. Rename columns for readability
df_cleaned = df.withColumnRenamed("signup_date", "registered_on")

# Show results
print("Transformed Data:")
df_cleaned.show(5)

print("Filtered Female Active:")
female_active.show(5)

print("Summary Count:")
summary.show()

spark.stop()
