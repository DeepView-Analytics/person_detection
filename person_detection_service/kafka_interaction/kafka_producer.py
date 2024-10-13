from kafka import KafkaProducer
import json
from serializers import serialize
from schemas import DetectionResponse

class KafkaProducerService:
    def __init__(self, bootstrap_servers='192.168.111.131:9092', topic='person_detection_responses'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else serialize(v)  # Serialize DetectionResponse
        )
        self.topic = topic
    def send_detection_response(self, request_id, detections):
        response_message = DetectionResponse(request_id=request_id, detection=detections)
        self.producer.send(self.topic, response_message)
        self.producer.flush()  # Ensure the message is sent
