import json
import logging
import time
from pprint import pformat
from quixstreams import Application
from requests_sse import EventSource, InvalidStatusCodeError

def handle_stats(stats_msg):
    stats = json.loads(stats_msg)
	logging.info("Producer stats: %s", pformat(stats))

def main():
    logging.info("START")
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        producer_extra_config={
            "statistics.interval.ms": 30000,
            "stats_cb": handle_stats, 
            "debug": "msg",
            "linger.ms": 200,  
            "batch.size": 5 * 1024 * 1024, 
            "compression.type": "gzip"             
		},
    )

    # Outer context manager keeps the Kafka producer pool open
    with app.get_producer() as producer:

        # Infinite loop keeps the processor alive through server drops
        while True:
            try:
                logging.info("Attempting to connect to firehose stream...")

                # Inner context manager handles the HTTP streaming connection
                with EventSource(
                    "https://github-firehose.libraries.io/events", timeout=30
                ) as event_source:

                    for event in event_source:
                        value = json.loads(event.data)
                        key = str(value["id"])
                        logging.debug("Got: %s", pformat(value))

                        producer.produce(
                            topic="github-events",
                            key=key,
                            value=json.dumps(value),
                        )

            except (InvalidStatusCodeError, Exception) as e:
                # Capture the 502 error gracefully, sleep 10s, and re-attempt
                logging.error("Stream disconnected or returned error: %s", e)
                logging.info("Retrying connection in 10 seconds...")
                time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    main()