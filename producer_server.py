from kafka import KafkaProducer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

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
            call = json.load(file)
            for line in dict_array:
                message = self.dict_to_binary(line)
                # TODO send the correct data
                print(f"message:" {message})
                self.produce(self.topic_name, message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict)



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
