
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dataclasses import dataclass, field

import json
import time


class ConsumerServer(Consumer):

    def __init__(self, topic_name_pattern, bootstrap_servers, group_id, offset_earliest=False,sleep_secs=1.0,consume_timeout=0.1):

        self.broker_properties = {
            "BROKER_URL" : bootstrap_servers
        }

        self.topic_name_pattern = topic_name_pattern
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        if self.offset_earliest:
            self.broker_properties['auto.offset.reset'] = 'earliest'

        super().__init__({
            "bootstrap.servers": self.broker_properties['BROKER_URL'],
            "group.id": group_id
        })

        if self.topic_exists():
            self.subscribe([self.topic_name_pattern], on_assign=self.on_assign)
        else
            raise KafkaException

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info("on_assign callback")

        if self.offset_earliest:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        self.assign(partitions)



    def topic_exists(self):
        """Checks if the given topic exists"""
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
        client = AdminClient({"bootstrap.servers": self.broker_properties['BROKER_URL']})
        cluster_metadata = client.list_topics()
        return cluster_metadata.topics.get(self.topic_name) is not None

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await time.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        while True:
            message = self.poll(self.consume_timeout)
            if message is None:
                logger.info("No message received by consumer")
                return 0
            elif message.error() is not None:
                logger.info(f"Error received {message.error()}")
                return 0
            else:
                self.process_message(message)
                return 1

    def process_message(self, message):
        """Process the incoming message"""
        if message.topic() == "com.udacity.police.sfo.calls":
            call_record = ServiceCall(json.loads(message.value()))

            print("Message Received!")
            print(f"{call_record.serialize()}")


    def close(self):
        """Cleans up any open kafka consumers"""
        if self is not None:
            self.close()




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
