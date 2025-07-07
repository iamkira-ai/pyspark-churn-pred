from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("RetailAnalytics-Ingestion") \
    .getOrCreate()

# Step 2: Load customers.csv into a DataFrame
df = spark.read.option("header", True).csv("../data/customers.csv")

# Step 3: Print Schema and Show Sample Rows
df.printSchema()
df.show(5)

# Step 4: Convert DataFrame to RDD and back (learning purpose)
rdd = df.rdd
df2 = rdd.toDF()

print("Total Records:", df2.count())

spark.stop()