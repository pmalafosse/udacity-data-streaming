from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
c = AvroConsumer({
    'bootstrap.servers': 'PLAINTEXT://localhost:9092',
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'earliest'
    },
    'schema.registry.url': 'http://localhost:8081'
})
c.subscribe(['^com.udacity.project1.arrivals.*'])
while True:
    try:
        msg = c.poll(10)
    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break
    if msg is None:
        continue
    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue
    print(msg.value())
c.close()