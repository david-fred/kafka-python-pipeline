import logging
import time
from requests_sse import EventSource, InvalidStatusCodeError

def main():
    logging.info("START")
    
    # Infinite loop keeps the processor alive through server drops
    while True:
        try:
            logging.info("Attempting to connect to firehose stream...")
            with EventSource(
                "https://github-firehose.libraries.io/events", timeout=30
            ) as event_source: 
                for event in event_source:
                    logging.info("Got: %s", event)
                    
        except (InvalidStatusCodeError, Exception) as e:
            # Capture the 502 error gracefully, sleep 10s, and re-attempt
            logging.error("Stream disconnected or returned error: %s", e)
            logging.info("Retrying connection in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()