import json
import logging
import sys
from datetime import datetime
from time import sleep

from kafka_controller import KafkaController

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

ex_topic = 'topic1'
con = KafkaController('producer')
# con.create_topic(NewTopic(ex_topic, 5))
# print(con.list_topic().topics)

while True:
    sleep(1)
    con.message(ex_topic, data=str(datetime.now()).encode('utf-8'))
