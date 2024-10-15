# Use a base image with Python installed
FROM ghcr.io/deepview-analytics/person_detection_env:latest 

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt first to install dependencies
COPY requirements.txt .

# Install necessary packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .


# Expose the port the app runs on
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
