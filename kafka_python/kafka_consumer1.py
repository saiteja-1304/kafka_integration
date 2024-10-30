from kafka import KafkaConsumer
import json

from sd_kafka import generate_data


# Kafka consumer configuration
consumer = KafkaConsumer(
    'data_generation_topic',
    bootstrap_servers='localhost:9092',  # Change to your Kafka server
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='data_generation_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Generation function to invoke per message
def process_generation_request(message):
    prompt_value = message.get("prompt")
    if prompt_value:
        print(f"Processing prompt: {prompt_value}")
        results =generate_data()
        # Do something with results (store, send response, etc.)
        print("Generated Data:", results)

# Consume messages in parallel
for message in consumer:
    msg_value = message.value
    print("Received message from Kafka:", msg_value)
    process_generation_request(msg_value)
