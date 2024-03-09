from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Produce binary values (0s and 1s) randomly
for _ in range(1000):
    value = random.choice([b'0', b'1'])
    producer.send('test-topic', value)
    time.sleep(1)