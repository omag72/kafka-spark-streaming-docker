import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "test-topic"

fake = Faker()

def generate_weather_data():
    weather_data = {
        "timestamp": int(time.time()),
        "location": fake.city(),
        "temperature": round(random.uniform(-10, 40), 2),
        "humidity": round(random.uniform(10, 90), 2),
        "weather_condition": fake.random_element(elements=("Sunny", "Cloudy", "Rainy", "Snowy")),
    }
    return json.dumps(weather_data)

def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BROKER,
    }

    producer = KafkaProducer(producer_config)

    try:
        while True:
            weather_data = generate_weather_data()
            print(weather_data)
            producer.produce(TOPIC, weather_data.encode("utf-8"), callback=delivery_report)
            producer.poll(0.5)  # Poll for events, adjust interval as needed
            time.sleep(2)  # Simulate generating data every 2 seconds
    except KeyboardInterrupt:
        pass

    producer.flush()
