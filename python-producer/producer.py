#from kafka import KafkaProducer


#TOPIC_NAME = 'mydata'
#KAFKA_SERVER = 'localhost:9092'

#producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
#producer.send('mydata',b'using python to execute msgs')
#producer.send('mydata',b'This is Kafka-Python, welcome')
#producer.flush()

#!/usr/bin/python3

from kafka import KafkaProducer
import json
import random
import time

# Kafka broker details
bootstrap_servers = 'localhost:9092'
topic = 'test-topic'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate IoT device data
def simulate_iot_device(device_id):
    while True:
        # Generate random sensor data
        temperature = round(random.uniform(20.0, 30.0), 2)
        humidity = round(random.uniform(40.0, 60.0), 2)
        pressure = round(random.uniform(900.0, 1100.0), 2)

        # Create JSON payload
        payload = {
            'device_id': device_id,
            'timestamp': int(time.time()),
            'temperature': temperature,
            'humidity': humidity,
            'pressure': pressure
        }

        # Send data to Kafka topic
        producer.send(topic, value=payload)
        print(f"Sent data: {payload}")

        # Sleep for a random interval (e.g., 1-5 seconds)
        time.sleep(random.uniform(1, 5))

# Start simulating IoT devices
device_ids = ['device1', 'device2', 'device3']  # Add more device IDs if needed
for device_id in device_ids:
    simulate_iot_device(device_id)


    
    