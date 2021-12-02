from typing import Union

from confluent_kafka import Consumer, Producer, Message
from confluent_kafka.admin import AdminClient, ClusterMetadata

bootstrap_servers = 'localhost:9092'
default_group_id = 'test_group'

admin_config = {
    'bootstrap.servers': bootstrap_servers,
    # 'group.id': group_id,
    # 'auto.offset.reset': 'earliest'
}

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': default_group_id,
    'auto.offset.reset': 'earliest'
}

producer_config = {
    'bootstrap.servers': bootstrap_servers,
    # 'group.id': group_id,
}


class KafkaController:
    def __init__(self, group_id=None):
        if group_id is not None:
            self.consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
            })
        else:
            self.consumer = Consumer(consumer_config)
        self.adminClient = AdminClient(admin_config)
        self.producer = Producer(producer_config)

    def create_topic(self, topic_obj):
        topics = [topic_obj, ]
        self.adminClient.create_topics(topics)
        return True

    def list_topic(self) -> ClusterMetadata:
        return self.adminClient.list_topics()

    def message(self, topic, data, callback=None):
        def message_check(err, msg: Message):
            print(err, msg.value())

        print(f'message {data}')
        self.producer.produce(topic=topic, value=data, callback=message_check)
        self.producer.poll(1.0)
        # self.producer.flush()

    def subscribe(self, topic: Union[str, list]):
        if isinstance(topic, str):
            topic = [topic, ]
        self.consumer.subscribe(topic)

    def read(self) -> Union[None, Message]:
        return self.consumer.poll(1.0)


