import pandas as pd
from faker import Faker
import random

fake = Faker()
num_records = 100

data = []

for _ in range(num_records):
    customer_id = fake.uuid4()
    name = fake.name()
    email = fake.email()
    signup_date = fake.date_between(start_date='-2y', end_date='today')
    gender = random.choice(['Male', 'Female'])
    status = random.choice(['active', 'churned'])

    data.append([customer_id, name, email, signup_date, gender, status])

df = pd.DataFrame(data, columns=['customer_id', 'name', 'email', 'signup_date', 'gender', 'status'])
df.to_csv('../data/customers.csv', index=False)

print("Generated customers.csv with", len(df), "records")
