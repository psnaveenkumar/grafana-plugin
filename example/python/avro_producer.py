# from kafka import KafkaProducer
import io
from avro.schema import parse
from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder
from confluent_kafka import Producer
import json
import random
import datetime
from time import sleep

# Create a Kafka client ready to produce messages
# bootstrap_address = "localhost:9092"
# producer = KafkaProducer(bootstrap_servers=bootstrap_address)
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

schema_def = {"type":"record", "name":"Data", "fields": [{"name":"name","type":"string"},{"name":"quality","type":"string"},{"name":"valuetimestamp","type":"string"}, {"name":"value","type":"double"}]}

schema = parse(json.dumps(schema_def).encode('utf-8'))
counter = 1
while True:
# serialize the message data using the schema
    ran = random.randint(0, 12)
    valuetimestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-ran))).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    myobject = {"value": random.randint(100, 200) ,
                "name": "name" + str(random.random()),
                "quality": "quality"+ str(random.random()),
                "valuetimestamp": valuetimestamp}

    buf = io.BytesIO()
    encoder = BinaryEncoder(buf)
    writer = DatumWriter(schema)
    writer.write(myobject, encoder)
    message_data = buf.getvalue()
    # message key if needed
    key = None
    # headers if needed
    headers = []
    topicname = "avro"
    # Send the serialized message to the Kafka topic
    producer.produce("avro", value=message_data)
    producer.flush()
    print("Sample #{} produced!".format(counter))
    counter += 1
    sleep(0.5)
