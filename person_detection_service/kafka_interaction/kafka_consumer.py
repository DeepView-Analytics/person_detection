from threading import Thread
from kafka import KafkaConsumer
from .serializers import deserialize

from service.person_detector import PersonDetector
from .kafka_producer import KafkaProducerService

class KafkaConsumerService:
    def __init__(self, bootstrap_servers='192.168.111.131:9092', topic='person_detection_requests'):
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
        try:
            for message in self.consumer:
                if not self.running:
                    break
                print("there is a message")
                request = message.value  # request is already deserialized to DetectionRequest
                detections = self.detector.detect_persons(request.frames) #return list of ObjectDetected object
                print("done_with model")
                self.producer.send_detection_response(request.request_id, detections)
        except Exception as e:
         print(f"Error: {e}")
        finally:
            self.consumer.close()  # Ensure the consumer closes properly

    def stop(self):
        self.running = False
        self.consumer.close()
