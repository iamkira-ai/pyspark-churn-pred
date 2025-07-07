import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("ChurnPrediction").getOrCreate()

# Load datasets
customers = spark.read.option("header", True).csv("../data/customers.csv")
transactions = spark.read.option("header", True).csv("../data/transactions.csv")

# Cast required fields
transactions = transactions.withColumn("quantity", col("quantity").cast("int")) \
                           .withColumn("price", col("price").cast("float")) \
                           .withColumn("total_spent", col("quantity") * col("price"))

# Join
joined = transactions.join(customers, on="customer_id", how="inner")

# Aggregate per customer
customer_features = joined.groupBy("customer_id", "gender", "status") \
    .agg(
        _sum("total_spent").alias("total_spent"),
        count("transaction_id").alias("transaction_count")
    )

# Label encoding: churned = 1, active = 0
data = customer_features.withColumn("label", when(col("status") == "churned", 1).otherwise(0))

# Index gender column
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_index")
data = gender_indexer.fit(data).transform(data)

# Assemble features into vector
assembler = VectorAssembler(
    inputCols=["gender_index", "total_spent", "transaction_count"],
    outputCol="features"
)
final_data = assembler.transform(data)

# Split dataset
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

# Train Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_data)

# Evaluate
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)

print("Model Accuracy (AUC):", round(accuracy, 4))
predictions.select("customer_id", "prediction", "probability", "label").show(10)

spark.stop()
