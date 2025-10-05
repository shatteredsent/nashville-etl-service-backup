# Use official Python runtime as base image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose port for API
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SCRAPY_SETTINGS_MODULE=nashville.settings

# Default command - runs the Flask/FastAPI app
CMD ["python", "app.py"]