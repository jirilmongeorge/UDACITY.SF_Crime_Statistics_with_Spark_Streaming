from kafka import KafkaProducer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dataclasses import dataclass, field

import json
import time


class ProducerServer(Producer):

    def __init__(self, input_file, topic, bootstrap_servers, client_id):
        self.input_file = input_file
        self.topic_name = topic
        self.broker_properties = {
            "BROKER_URL" : bootstrap_servers
        }

        super().__init__({
            "bootstrap.servers": self.broker_properties['BROKER_URL'],
            "client.id": client_id,
        })

        self.create_topic()

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as file:
            calls = json.load(file)
            for call in calls:
                message = self.dict_to_binary(ServiceCall(call).serialize())
                # TODO send the correct data
                self.produce(self.topic_name, message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json_dict.encode('utf-8')



    def topic_exists(self, client):
        """Checks if the given topic exists"""
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
        cluster_metadata = client.list_topics()
        return cluster_metadata.topics.get(self.topic_name) is not None


    def create_topic(self):
        """Creates the topic with the given topic name"""
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
        # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
        client = AdminClient({"bootstrap.servers": self.broker_properties['BROKER_URL']})

        exists = self.topic_exists(client)

        if exists is False:
            futures = client.create_topics(
                [ NewTopic(
                     topic = self.topic_name,
                     num_partitions = 3,
                     replication_factor = 1,
                     )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Topic {self.topic_name} created!")
                except Exception as e:
                    print(f"Topic already created! {self.topic_name}")


@dataclass
class ServiceCall:

    def __init__(self, call):
        self.crime_id = call.get('crime_id')
        self.original_crime_type_name = call.get('original_crime_type_name')
        self.report_date = call.get('report_date')
        self.call_date = call.get('call_date')
        self.offense_date = call.get('offense_date')
        self.call_time = call.get('call_time')
        self.call_date_time = call.get('call_date_time')
        self.disposition = call.get('disposition')
        self.address = call.get('address')
        self.city = call.get('city')
        self.state = call.get('state')
        self.agency_id = call.get('agency_id')
        self.address_type = call.get('address_type')
        self.common_location = call.get('common_location')


    def serialize(self):
        return json.dumps(
            {
                "crime_id": self.crime_id,
                "original_crime_type_name": self.original_crime_type_name,
                "report_date": self.report_date,
                "call_date": self.call_date,
                "offense_date": self.offense_date,
                "call_time": self.call_time,
                "call_date_time": self.call_date_time,
                "disposition": self.disposition,
                "address": self.address,
                "city": self.city,
                "state": self.state,
                "agency_id": self.agency_id,
                "address_type": self.address_type,
                "common_location": self.common_location
            }
        )
