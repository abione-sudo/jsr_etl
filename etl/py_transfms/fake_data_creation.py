from dataclasses import dataclass
from faker import Faker
import os

@dataclass
class user:
    name:str
    order_id:int
    order_date:str
    amount:int

fake=Faker()

def user_generator():
    for _ in range(1000):
        yield user(fake.name(),fake.random_int(min=100,max=1000),fake.date(),fake.random_int(min=100,max=1000))



with open(f'/Users/abeee/data_engineering/data/raw_data/users.csv','w') as f:
     f.write("name,order_id,order_date,amount\n")
     for row in user_generator():
        f.write(f"{row.name},{row.order_id},{row.order_date},{row.amount}\n")