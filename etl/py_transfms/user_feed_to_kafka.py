from dataclasses import dataclass, asdict
from confluent_kafka import Producer
from faker import Faker
from datetime import datetime
import time, json

@dataclass
class user_info:
    user_id: int
    user_name: str
    site_id: int
    down_speed: int
    up_speed: int
    tickets_opened: int
    is_active: bool
    state: str
    country: str
    created_at: str

def generate_record(fake: Faker) -> user_info:
    return user_info(
        user_id=fake.random_int(min=1, max=10000),
        user_name=fake.user_name(),
        is_active=fake.boolean(chance_of_getting_true=80),
        site_id=fake.random_int(min=1, max=50),
        down_speed=fake.random_int(min=10, max=1000),
        up_speed=fake.random_int(min=5, max=500),
        state=fake.state(),
        country=fake.country(),
        tickets_opened=fake.random_int(min=0, max=10),
        created_at=str(datetime.utcnow())
    )

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

if __name__ == "__main__":
    fake = Faker()

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'compression.type': 'snappy'
    }

    p = Producer(conf)

    for _ in range(1000):
        record = generate_record(fake)
        key_str = json.dumps({"user_id": record.user_id})
        value_str = json.dumps(asdict(record))

        p.produce(
            topic='user_stats_topic',
            key=key_str,
            value=value_str,
            callback=delivery_report
        )
        p.poll(0)
        time.sleep(2)

    p.flush()
