from threading import Thread
from kafka import KafkaConsumer
from .serializers import deserialize

from service.person_detector import PersonDetector
from kafka_producer import KafkaProducerService

class KafkaConsumerService:
    def __init__(self, bootstrap_servers='localhost:9092', topic='person_detection_requests'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: deserialize(x.decode('utf-8')) 
        )
        self.detector = PersonDetector()
        self.running = True
        self.producer = KafkaProducerService()

    def start(self):
        Thread(target=self.consume_messages, daemon=True).start()

    def consume_messages(self):
        while self.running:
            for message in self.consumer:
                try:
                    request = message.value  # request is already deserialized to DetectionRequest
                    detections = self.detector.detect_persons(request.frames) #return list of ObjectDetected object
                    self.producer.send_detection_response(request.request_id, detections)
                except Exception as e:
                    print(f"Error processing message: {e}")

    def stop(self):
        self.running = False
        self.consumer.close()
