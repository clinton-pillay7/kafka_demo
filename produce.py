from kafka import KafkaProducer
import json
import time
import datetime
from faker import Faker

KAFKA_TOPIC = "demotopic"
BOOTSTRAP_SERVER = 'localhost:9092'

fake = Faker()


def get_random_invoice():
    tax_percentage = 15
    pay_methods = ['Cash', 'Card payment', 'EFT', 'bitcoin']
    price_ex = fake.random_int(min=3, max=15)
    date_time = fake.date_time_between(start_date='-20y', end_date='now', tzinfo=None) \
        .strftime('%Y-%m-%dT%H:%M:%S')
    return {
        "uuid": fake.random_number(digits=12),
        "name": fake.name(),
        "date": date_time,
        "address": fake.address().replace("\n", ", "),
        "payment_method": fake.random_element(elements=pay_methods),
        "price": {
            "net": price_ex,
            "taxPercentage": tax_percentage,
            "total": price_ex + (price_ex * (tax_percentage / 100))
        }
    }



def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    while 1:
        random_invoice = get_random_invoice()
        print(random_invoice)
        producer.send(KAFKA_TOPIC, random_invoice)
        time.sleep(fake.random_int(0, 3))
