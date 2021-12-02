from random import randint

from kafka_controller import KafkaController

app_num = randint(0, 1000)
con = KafkaController(group_id=f'test_group_{1}')

topic = 'topic1'


def consume_loop():

    con.subscribe(topic)

    while True:
        msg = con.read()
        if msg is not None:
            print(app_num, msg.value().decode('utf8'), msg.offset())
            con.consumer.commit(asynchronous=False)


if __name__ == '__main__':
    consume_loop()