import base64
import os
import sys
import json
from unittest import TestCase
import unittest
from unittest.mock import MagicMock, patch

# Ensure the path to your project modules is set correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../kafka_interaction')))
from kafka_interaction.kafka_producer import KafkaProducerService
from kafka_interaction.kafka_consumer import KafkaConsumerService
from schemas import ObjectDetected  # Ensure this import matches your project structure

class TestKafkaConsumerService(TestCase):

    @patch('kafka_interaction.kafka_producer.KafkaProducerService')  # Mock the KafkaProducerService
    @patch('kafka_interaction.kafka_consumer.KafkaConsumer')  # Mock the KafkaConsumer
    def test_consume_messages(self, mock_kafka_consumer, mock_kafka_producer):
        # Arrange
        frame_1 = base64.b64encode(bytes([14, 70, 54])).decode('utf-8')
        frame_2 = base64.b64encode(bytes([12, 100, 85])).decode('utf-8')
        mock_message = json.dumps({
            "request_id": 1,
            "frames": [
                {
                    "camera_id": 1,
                    "frame": frame_1
                },
                {
                    "camera_id": 2,
                    "frame": frame_2 
                }
            ]
        })

        # Set the return value for the mock Kafka consumer to simulate message consumption
        mock_kafka_consumer.return_value.__iter__.return_value = iter([mock_message])
        
        # Mock the behavior of your object detector (assuming you have a PersonDetector class)
        mock_detector = MagicMock()
        mock_detector.detect_persons.return_value = [
            ObjectDetected(camera_id=1, object_detected=[20, 56, 1210, 1600, 0.8972072005271912]),
            ObjectDetected(camera_id=2, object_detected=[61, 401, 500, 857, 0.4654568135738373]),
            ObjectDetected(camera_id=2, object_detected=[453, 56, 625, 313, 0.9238410592079163]),
            ObjectDetected(camera_id=2, object_detected=[127, 47, 294, 319, 0.9159606695175171]),
            ObjectDetected(camera_id=2, object_detected=[321, 49, 484, 313, 0.9154051542282104])
        ]


        # Act
        mock_kafka_consumer.consume_messages()  # This method should handle the mocked message

        # Assert
        # Ensure that the producer sends a response with the detections
        mock_kafka_producer.return_value.send.assert_called_once()
        
        # Extract the sent message and verify its contents
        sent_request_id, sent_detections = mock_kafka_producer.return_value.send.call_args[0][1]
        self.assertEqual(sent_request_id, 1)
        self.assertEqual(len(sent_detections), 5)  # Check number of detections
        
        # Verify the content of detections
        self.assertEqual(sent_detections[0].camera_id, 1)
        self.assertEqual(sent_detections[0].object_detected, [20, 56, 1210, 1600, 0.8972072005271912])
        self.assertEqual(sent_detections[1].camera_id, 2)
        self.assertEqual(sent_detections[1].object_detected, [61, 401, 500, 857, 0.4654568135738373])
        self.assertEqual(sent_detections[2].camera_id, 2)
        self.assertEqual(sent_detections[2].object_detected, [453, 56, 625, 313, 0.9238410592079163])
        self.assertEqual(sent_detections[3].camera_id, 2)
        self.assertEqual(sent_detections[3].object_detected, [127, 47, 294, 319, 0.9159606695175171])
        self.assertEqual(sent_detections[4].camera_id, 2)
        self.assertEqual(sent_detections[4].object_detected, [321, 49, 484, 313, 0.9154051542282104])

if __name__ == '__main__':
    unittest.main()
