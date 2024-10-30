import datetime
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
from kafka.errors import KafkaError
from flask import current_app, jsonify
import threading
from queue import Queue

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'generate_data_topic'
KAFKA_RESPONSE_TOPIC = 'generate_data_response_topic'

class KafkaHandler:
    def __init__(self, flask_app):
        self.app = flask_app
        self.producer = None
        self.consumer = None
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.message_queue = Queue()
        self.initialize_kafka()

    def initialize_kafka(self):
        """Initialize Kafka producer and consumer with error handling"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            logger.info("Kafka producer initialized successfully")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def produce_generation_requests(self, num_requests=1000):
        """Produce generation requests to Kafka topic"""
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return

        for i in range(num_requests):
            message = {
                "request_id": f"req_{i}",
                "timestamp": datetime.datetime.now().isoformat()
            }
            try:
                future = self.producer.send(KAFKA_TOPIC, value=message)
                future.get(timeout=10)  # Wait for sending confirmation
                logger.info(f"Successfully produced message: {message}")
            except KafkaError as e:
                logger.error(f"Failed to send message {i}: {e}")

    def process_generation_request(self, message):
        """Process a single generation request within Flask context"""
        with self.app.app_context():
            try:
                from sd_kafka import generate_data
                request_id = message.get("request_id")
                logger.info(f"Processing request ID: {request_id}")
                
                # Call generate_data and extract the JSON data
                response = generate_data()
                if isinstance(response, tuple):
                    response_data = response[0].get_json() if hasattr(response[0], 'get_json') else response[0]
                else:
                    response_data = response.get_json() if hasattr(response, 'get_json') else response
                
                # Ensure response_data is JSON serializable
                if isinstance(response_data, (dict, list)):
                    response_message = {
                        "request_id": request_id,
                        "status": "success",
                        "results": response_data
                    }
                    self.producer.send(KAFKA_RESPONSE_TOPIC, value=response_message)
                    logger.info(f"Generated and sent results for request {request_id}")
                else:
                    raise ValueError(f"Invalid response data type: {type(response_data)}")
                
            except Exception as e:
                logger.error(f"Error processing request: {e}")
                error_message = {
                    "request_id": message.get("request_id"),
                    "status": "error",
                    "error": str(e)
                }
                self.producer.send(KAFKA_RESPONSE_TOPIC, value=error_message)

    # def consume_and_generate(self):
    #     """Consume messages and process them"""
    #     try:
    #         self.consumer = KafkaConsumer(
    #             KAFKA_TOPIC,
    #             bootstrap_servers=[KAFKA_BROKER],
    #             group_id="generation_group",
    #             value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    #             auto_offset_reset='earliest',
    #             enable_auto_commit=True
    #         )
            
    #         logger.info("Started consuming messages")
            
    #         for message in self.consumer:
    #             try:
    #                 data = message.value
    #                 logger.info(f"Received message: {data}")
    #                 self.executor.submit(self.process_generation_request, data)
    #             except Exception as e:
    #                 logger.error(f"Error processing message: {e}")
                    
    #     except KafkaError as e:
    #         logger.error(f"Kafka consumer error: {e}")
    #     finally:
    #         if self.consumer:
    #             self.consumer.close()
    #         self.executor.shutdown(wait=True)
    
    def consume_and_generate(self):
        """Consume messages and process them"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id="generation_group",
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            logger.info("Started consuming messages")
            
            message_count = 0
            MAX_MESSAGES = 15  # Set limit to 5 messages
            
            for message in self.consumer:
                if message_count >= MAX_MESSAGES:
                    logger.info(f"Reached maximum message count of {MAX_MESSAGES}")
                    break
                    
                try:
                    data = message.value
                    logger.info(f"Processing message {message_count + 1} of {MAX_MESSAGES}")
                    logger.info(f"Received message: {data}")
                    self.executor.submit(self.process_generation_request, data)
                    message_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            logger.info(f"Consumer finished processing {message_count} messages")
                        
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            self.executor.shutdown(wait=True)

    def start(self):
        """Start the Kafka handler"""
        consumer_thread = threading.Thread(target=self.consume_and_generate)
        consumer_thread.daemon = True
        consumer_thread.start()
        logger.info("Kafka handler started")

def initialize_kafka_integration(flask_app):
    """Initialize and start Kafka integration with the Flask app"""
    kafka_handler = KafkaHandler(flask_app)
    kafka_handler.start()
    return kafka_handler