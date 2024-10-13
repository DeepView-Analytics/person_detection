import base64
import json
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../kafka_interaction')))

from schemas import DetectionRequest, DetectionResponse

def serialize(request: DetectionResponse) -> str:
    """Serialize a DetectionResponse object to a JSON string for Kafka."""
    return request.model_dump_json()  # Using Pydantic's new method to convert to JSON

def deserialize(message: str) -> DetectionRequest:
    """Deserialize a JSON string from Kafka to a DetectionRequest object."""
    # Load the JSON message into a dictionary
    parsed_message = json.loads(message)

    # Iterate over frames and decode Base64-encoded string to bytes
    for frame in parsed_message['frames']:
        frame['frame'] = base64.b64decode(frame['frame'])  # Decode Base64 to bytes

    # Rebuild the DetectionRequest object with decoded frames
    return DetectionRequest(**parsed_message)