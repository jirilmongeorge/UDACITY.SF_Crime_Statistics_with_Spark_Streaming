import producer_server
import consumer_server
from pathlib import Path
import asyncio



def run_kafka_server():
	# TODO get the json file path
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.police.sfo.calls",
        bootstrap_servers="PLAINTEXT://localhost:9092",
        client_id="kafka-server-calls"
    )

    return producer


def feed():
    calls_producer = run_kafka_server()
    calls_producer.generate_data()



if __name__ == "__main__":
    feed()
