import logging
from kafka import KafkaProducer
from .serializers import serialize
from .schemas import DetectionResponse, ObjectDetected



class KafkaProducerService:
    def __init__(self, bootstrap_servers='127.0.0.1:9092', topic='person_detection_response'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            api_version = (7,3,2),
            value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v ,  # Ensure it's bytes
            max_request_size=10485760
        )
        self.topic = topic

    def send_detection_response(self, request_id, detections):
        response_message = DetectionResponse(request_id=request_id, detection=detections)
        # Serialize the message using your serialize() function
        serialized_message = serialize(response_message)  # This returns a JSON string
        
        # Convert the serialized message to bytes
        serialized_message_bytes = serialized_message.encode('utf-8')  # Encode to UTF-8 bytes
        
        message_size = len(serialized_message_bytes)
        print(f"Message size: {message_size} bytes")

        if message_size > 10485760:  # Check if the message exceeds 10 MB
            print(f"Message size exceeds limit of 10 MB.")
            return

        print("Attempting to send message to topic")
        try:
            future = self.producer.send(self.topic, serialized_message_bytes)  
            record_metadata = future.get(timeout=10)
            print(f"Message sent successfully to topic {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}", exc_info=True)
        finally:
            self.producer.flush() 

    def close(self):
        self.producer.flush()
        self.producer.close()

