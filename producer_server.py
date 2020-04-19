from kafka import KafkaProducer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import json
import time


class ProducerServer(Producer):

    def __init__(self, input_file, topic, bootstrap_servers, client_id):
        self.input_file = input_file
        self.topic = topic
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
            dict_array = json.load(file)
            for line in dict_array:
                message = self.dict_to_binary(dict_array)
                # TODO send the correct data
                self.produce(self.topic, message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(
        {
            "crime_id": json_dict['crime_id'],
            "original_crime_type_name": json_dict['original_crime_type_name'],
            "report_date": json_dict['report_date'],
            "call_date": json_dict['call_date'],
            "offense_date": json_dict['offense_date'],
            "call_time": json_dict['call_time'],
            "call_date_time": json_dict['call_date_time'],
            "disposition": json_dict['disposition'],
            "address": json_dict['address'],
            "city": json_dict['city'],
            "state": json_dict['state'],
            "agency_id": json_dict['agency_id'],
            "address_type": json_dict['address_type'],
        }).encode('utf-8')



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
