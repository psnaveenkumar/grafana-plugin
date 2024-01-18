"""
This script is a simple sample generator for Kafka.
The provided Kafka Datasource visualizes those samples in Grafana.
"""

from confluent_kafka import Producer
from time import sleep
import random
import json
import datetime


conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

counter = 1

while True:
    ran = random.randint(0, 12) 
    x = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-ran))).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    data = {"value": random.randint(100, 200) ,
            "name": "name" + str(random.random()),
            "quality": "quality"+ str(random.random()),
            "valuetimestamp": x}        
    producer.produce("test", value=json.dumps(data))
    print("Sample #{} produced!".format(counter))
    print("time #{} produced!".format(x))
    print("time #{} produced!".format(random.random()))
    counter += 1
    producer.flush(1)
    sleep(0.5)
