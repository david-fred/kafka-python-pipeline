from quixstreams import Application
import json
import logging

logging.basicConfig(level=logging.INFO)

app= Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
    consumer_group="weather_reader_group",
)

with app.get_consumer() as consumer:
    consumer.subscribe(["weather_data_demo"])

    while True:
        msg = consumer.poll(1)

        if msg is None: 
            print("Waiting for messages...")
            continue

        if msg.error() is not None:
            print(f"Kafka error occurred: {msg.error()}")
            continue
      
        breakpoint()
        

    