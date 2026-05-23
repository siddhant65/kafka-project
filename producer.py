import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f" Delivery failed: {err}")
    else:
        print(f" Delivered {msg.value().decode('utf-8')}")
        print(f" Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")
        print(dir(msg))

order = {
    "order_id": str(uuid.uuid4()),
    "user": "siddhant",
    "item": "burger",
    "quantity": 3
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()

""" To run in terminal :
Validate that the topic was created in kafka container:
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

Describe that topic and see its partitions:
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

View all events in a topic:
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
"""
