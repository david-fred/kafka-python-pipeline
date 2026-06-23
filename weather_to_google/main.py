import logging
from quixstreams import Application
from uuid import uuid4
from datetime import timedelta
import pygsheets 

def initializer_fn(msg):
	temperature = msg['current']['temperature_2m']
	
	return {
		"open":temperature,
		"high":temperature,
		"low":temperature,
		"close": temperature,
	}


def reducer_fn(summary, msg):
	temperature = msg['current']['temperature_2m']
	
	return {
		"open": summary["open"],
		"high": max(summary["high"], temperature),
		"low": min(summary["low"], temperature),
		"close": temperature,
	}

def main():
	app = Application(
		broker_address="localhost:9092",
		loglevel="DEBUG",
		
		consumer_group=str(uuid4()),
		auto_offset_reset="earliest",
	)

	input_topic = app.topic("weather_data_demo")

	sdf = app.dataframe(input_topic)
	
	# sdf = sdf.group_into_hourly_batches(..)
	sdf = sdf.tumbling_window(duration_ms=timedelta(hours=1))
		
	#sdf = sdf.summarize_that_hour(..)
	sdf = sdf.reduce(
		initializer=initializer_fn,
		reducer= reducer_fn,
	).final()

	sdf.update(lambda msg: logging.debug("Got: %s", msg))
	
	#sdf = sdf.send_to_google_sheets(..)
	

	app.run(sdf)


if __name__ == "__main__":
	logging.basicConfig(level="DEBUG")
	#main() 

	google_api = pygsheets.authorize(service_account_file="extended-ascent-472512-j1-5d4b3cf0890f.json")
	workspace = google_api.open('Weather Data Dashboard')
	print(workspace)
	
