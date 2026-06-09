import logging
from quixstreams import Application
from uuid import uuid4

def main():
	app = Application(
		broker_address="localhost:9092",
		loglevel="DEBUG",
		
		consumer_group=str(uuid4()),
		auto_offset_reset="earliest",
	)

	input_topic = app.topic(weather_data_demo)

	sdf = app.dataframe(input_topic)
	
	# sdf = sdf.group_into_hourly_batches(..)
	sdf = sdf.summarize_that_hour(..)
	sdf = sdf.send_to_google_sheets(..)
	

	app.run(sdf)


if __name__ == "__main__":
	logging.basicConfig(level="DEBUG")
	main()
