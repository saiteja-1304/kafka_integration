from kafka import KafkaProducer
import json
import time

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Change to your Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send generation requests
def send_generation_request(prompt_value):
    message = {
        "prompt": prompt_value,
        "timestamp": time.time()
    }
    producer.send('data_generation_topic', message)
    producer.flush()
    print("Message sent to Kafka:", message)

# Send 5 messages to trigger 5 parallel generations
for _ in range(5):
    send_generation_request("Your prompt for data generation here")
