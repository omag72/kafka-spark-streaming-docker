from kafka import KafkaProducer
import json
import time
import random

# Kafka broker address
KAFKA_BROKER = "localhost:9092"

# Kafka topic to produce to
TOPIC = "test-topic"

def generate_data():
    data = {
        "timestamp": int(time.time()),
        "value": round(random.uniform(0, 100), 2),
        "status": random.choice(["normal", "warning", "error"]),
    }
    return json.dumps(data)

if __name__ == "__main__":
    # Create a Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    try:
        while True:
            # Generate data
            message = generate_data()

            # Produce the message to the specified topic
            producer.send(TOPIC, value=message)

            print("Message sent:", message)
            time.sleep(2)  # Simulate generating data every 2 seconds

    except KeyboardInterrupt:
        pass

    # Close the producer
    producer.close()



    