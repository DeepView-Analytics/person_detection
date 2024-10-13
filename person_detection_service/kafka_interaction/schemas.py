# kafka/schemas.py
from pydantic import BaseModel
from typing import List, Dict, Any

class Frame(BaseModel):
    camera_id: int
    frame: bytes 

class DetectionRequest(BaseModel):
    request_id: int
    frames: List[Frame]

class ObjectDetected(BaseModel):
    camera_id: int
    object_detected: List[Any] 
     

class DetectionResponse(BaseModel):
    request_id: int
    detection: List[ObjectDetected]