from datetime import datetime
from kafka import KafkaConsumer
import sys
from pymongo.mongo_client import MongoClient
import json

uri = "mongodb://192.168.8.190:27017/sales_data"
# Create a new client and connect to the server
client = MongoClient(uri)

# Step 2: Select the database and collection
db = client['sales_data']  # Replace with your database name
collection = db['purchases']  # Replace with your collection name

KAFKA_TOPIC = "demotopic"
BOOTSTRAP_SERVER = 'localhost:9092'
GROUP_ID = 'consumerGroup1'
AUTO_OFFSET_RESET = 'earliest'

if __name__ == "__main__":
    consumer = KafkaConsumer(KAFKA_TOPIC,
                             group_id=GROUP_ID,
                             bootstrap_servers=BOOTSTRAP_SERVER,
                             auto_offset_reset=AUTO_OFFSET_RESET,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    try:
        for message in consumer:
            record = message.value
            #print(type(record))
            collection.insert_one(record)


    except KeyboardInterrupt:
        client.close()
        sys.exit()
