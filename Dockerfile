# Use an official lightweight Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Set environment variables for Python to run in an optimized mode
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's code into the container
COPY . .

# Command to run the application
# Google Cloud Run will set the PORT environment variable.
# Uvicorn will listen on 0.0.0.0 and the specified port.
# Use gunicorn to run Flask
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "6000", "main:app"]
