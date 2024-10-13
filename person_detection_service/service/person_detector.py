import os
import sys
from typing import List
import torch
from PIL import Image
from io import BytesIO
from typing import List, Dict
import numpy as np
import cv2
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../kafka_interaction')))

from schemas import Frame, ObjectDetected

class PersonDetector:
    def __init__(self):
        """
        Initialize the PersonDetector with a model.
        
        Args:
            model: An instance of your detection model (e.g., YOLOv5).
        """
        self.model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

    def detect_persons_model_interaction(self, bytearray_images: List[bytearray], target_size=(640, 640)) -> List[List[Dict[str, float]]]:
        """
        Detect persons in a batch of images using YOLOv5 model.

        Args:
            model: The YOLOv5 model.
            bytearray_images: List of images in bytearray format.
            target_size: The target size to which images will be resized (default is 640x640).

        Returns:
            List of lists containing bounding boxes for persons detected in each image.
        """

        def resize_image_with_padding(image, target_size):
            h, w = image.shape[:2]
            scale = min(target_size[0] / h, target_size[1] / w)
            nh, nw = int(h * scale), int(w * scale)
            image_resized = cv2.resize(image, (nw, nh))
            new_image = np.zeros((target_size[0], target_size[1], 3), dtype=np.uint8)
            new_image[:nh, :nw] = image_resized
            return new_image, scale, (nw, nh)

        def adjust_bboxes(bboxes, scale):
            adjusted_bboxes = []
            for bbox in bboxes:
                xmin, ymin, xmax, ymax, conf = bbox
                xmin = int(xmin / scale)
                ymin = int(ymin / scale)
                xmax = int(xmax / scale)
                ymax = int(ymax / scale)
                adjusted_bboxes.append({
                    "xmin": xmin,
                    "ymin": ymin,
                    "xmax": xmax,
                    "ymax": ymax,
                    "confidence": conf
                })
            return adjusted_bboxes

        # Convert bytearray images to PIL images and then to numpy arrays
        images = [cv2.cvtColor(np.array(Image.open(BytesIO(img))), cv2.COLOR_RGB2BGR) for img in bytearray_images]

        # Resize images and keep track of scales
        resized_images = []
        scales = []
        for img in images:
            resized_img, scale, _ = resize_image_with_padding(img, target_size)
            resized_images.append(resized_img)
            scales.append(scale)


        # Perform inference
        results = self.model(resized_images)

        # Extract and adjust bounding boxes for persons from `Detections` object
        person_bboxes = []
        for idx, result in enumerate(results.xyxy):  # Access xyxy format results from the Detections object
            bboxes = []
            if len(result) == 0:
                person_bboxes.append([])
                continue
            for box in result.tolist():
                xmin, ymin, xmax, ymax, conf, cls = box
                if int(cls) == 0:  # Class 0 is 'person' in COCO dataset
                    bboxes.append([xmin, ymin, xmax, ymax, conf])
            adjusted_bboxes = adjust_bboxes(bboxes, scales[idx])
            person_bboxes.append(adjusted_bboxes)

        return person_bboxes
        
    def detect_persons(self, frames: List[Frame]) -> List[ObjectDetected]:
        """
        Process a batch of frames and detect persons in each frame.

        Args:
            frames: A list of Frame objects containing camera_id and the actual frame data.

        Returns:
            A list of ObjectDetected instances mapping camera_id to detected objects.
        """
        print(f"len of frams in service : {len(frames)}")
        for frame in frames:
           print(type(frame))
        
        # Extract the frames (byte data) from the Frame objects
        frame_data = [frame.frame for frame in frames]  # This should give you the bytearray images

        
        # Run the model on the batch of frames
        detections = self.detect_persons_model_interaction(frame_data)  #detections is a list of list of bboxs ( person detected in the frame)
        print(f"len detection = {len(detections)}")
        # Map detections back to their respective camera IDs
        results = []
        for idx, frame in enumerate(frames):

            camera_id = frame.camera_id
            # Assume detections[idx] holds the detections for the corresponding frame
            results.append(ObjectDetected(camera_id=camera_id, object_detected=detections[idx]))
        
        return results