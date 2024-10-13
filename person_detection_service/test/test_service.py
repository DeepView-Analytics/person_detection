import sys
import os

# Add the project root directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../kafka_interaction')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../service')))
from schemas import Frame, ObjectDetected
from person_detector import PersonDetector  

def test_person_detector():
    # Initialize the person detector
    detector = PersonDetector()

    # Example images to test (Replace these with your actual image paths)
    image_paths = ['test/1.jpg', 'test/2.jpg', 'test/5.jpg','test/3.jpg','test/4.png']  # Replace with actual image paths
    frames = []

    # Load images and create Frame objects
    for i, img_path in enumerate(image_paths):
        with open(img_path, 'rb') as img_file:
            img_bytes = img_file.read()
            # Create a Frame object with a unique camera_id for each image
            frame = Frame(camera_id=int(i), frame=bytearray(img_bytes))
            frames.append(frame)
    print(len(frames))
    # Perform detection
    results = detector.detect_persons(frames)

    # Print out the detection results
    for result in results:
        print(f"Camera ID: {result.camera_id}")
        print(f"Detected Objects: {result.object_detected}")

# Run the test
if __name__ == "__main__":
    test_person_detector()
