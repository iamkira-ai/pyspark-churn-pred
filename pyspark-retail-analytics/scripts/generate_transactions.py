import pandas as pd
from faker import Faker
import random
from datetime import timedelta
import uuid

fake = Faker()
num_records = 500

products = ["TV", "Laptop", "Phone", "Headphones", "Charger"]

# Load customer IDs
customers_df = pd.read_csv("../data/customers.csv")
customer_ids = customers_df["customer_id"].tolist()

data = []

for _ in range(num_records):
    transaction_id = str(uuid.uuid4())
    customer_id = random.choice(customer_ids)
    product = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(50, 2000), 2)
    date = fake.date_between(start_date='-1y', end_date='today')

    data.append([transaction_id, customer_id, product, quantity, price, date])

df = pd.DataFrame(data, columns=["transaction_id", "customer_id", "product", "quantity", "price", "date"])
df.to_csv("../data/transactions.csv", index=False)

print("Generated transactions.csv with", len(df), "records.")