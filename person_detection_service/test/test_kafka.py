import base64
import os
import time
import cv2
import json
from typing import List

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel
load_dotenv()
print(os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.111.131:9092'))
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.111.131:9092')
KAFKA_TOPIC_REQUESTS = os.getenv('KAFKA_TOPIC_REQUESTS', 'person_detection_requests')

# Define Frame and DetectionRequest models
class Frame(BaseModel):
    camera_id: int
    frame: str  # Use `str` since Base64 encoding produces a string

class DetectionRequest(BaseModel):
    request_id: int
    frames: List[Frame]

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize to JSON and encode to bytes
    api_version = (7,3,2),
    max_request_size=10485760
)

def load_images_as_bytes(image_paths):
    frames = []
    for idx, image_path in enumerate(image_paths):
        # Read the image using OpenCV
        image = cv2.imread(image_path)
        
        if image is None:
            print(f"Failed to read image: {image_path}")
            continue
        
        # Encode image to PNG format as bytes
        success, encoded_image = cv2.imencode('.png', image)
        
        if success:
            # Convert image bytes to Base64 string
            encoded_image_str = base64.b64encode(encoded_image.tobytes()).decode('utf-8')
            frame = Frame(camera_id=idx, frame=encoded_image_str)
            frames.append(frame)
        else:
            print(f"Failed to encode image: {image_path}")
    return frames

def produce_detection_request(image_paths):
    # Load images and convert to frames
    frames = load_images_as_bytes(image_paths)
    
    # Create DetectionRequest object
    detection_request = DetectionRequest(
        request_id=1,  # You can generate or increment request IDs as needed
        frames=frames
    )
    
    # Send to Kafka, using Pydantic's .model_dump() to convert the object to a dictionary
    future = producer.send(KAFKA_TOPIC_REQUESTS, detection_request.model_dump())
    try:
        record_metadata = future.get(timeout=10)  # Wait for a response (timeout in seconds)
        print(f"Message sent successfully to topic {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")
    except Exception as e:
        print(f"Failed to send message: {e}")
    producer.flush()
    print(f"Detection request with {len(frames)} frames sent to Kafka topic {KAFKA_TOPIC_REQUESTS}")

if __name__ == '__main__':
    # Test with 5 images (provide the paths of 5 images here)
    image_paths = [
        '1.jpg',
        '2.jpg',
        '3.jpg',
        '4.png',
        '5.jpg'
    ]

    # Produce detection request
    produce_detection_request(image_paths)
    
