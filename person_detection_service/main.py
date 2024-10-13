import os
from fastapi import FastAPI
from kafka_interaction.kafka_consumer import KafkaConsumerService
from kafka_interaction.kafka_producer import KafkaProducerService
import uvicorn
from contextlib import asynccontextmanager

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.111.129:9092')
KAFKA_TOPIC_REQUESTS = os.getenv('KAFKA_TOPIC_REQUESTS', 'person_detection_requests')
KAFKA_TOPIC_RESPONSES = os.getenv('KAFKA_TOPIC_RESPONSES', 'person_detected_responses')  

# Initialize Kafka Producer and Consumer
producer = KafkaProducerService(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic= KAFKA_TOPIC_RESPONSES 
    )
consumer = KafkaConsumerService(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC_REQUESTS
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    consumer.start()
    yield
    # Shutdown logic
    consumer.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Person Detection Service!"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv('FASTAPI_PORT', 8000)))
