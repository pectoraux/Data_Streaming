from pykafka.simpleconsumer import OffsetType

from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092") #kafka consumer client

topic = client.topics["service-calls"] #topic to read from

consumer_messages = topic.get_balanced_consumer(
    consumer_group = b'pytkafka-data',
    zookeeper_connect = 'localhost:2181'
)

#Print out the messages
for msg in consumer_messages:
    if msg is not None:
        print( msg.value.decode('utf-8'))
