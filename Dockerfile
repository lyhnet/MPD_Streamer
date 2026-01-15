# Use lightweight Python image
FROM python:3.11-slim

# Prevent .pyc files & enable stdout logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside container
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose the port your app uses
EXPOSE 5000

# Default environment variables (can be overridden)
ENV UVICORN_HOST=0.0.0.0
ENV UVICORN_PORT=5000

CMD ["sh", "-c", "uvicorn f_streamer:app --host ${UVICORN_HOST} --port ${UVICORN_PORT}"]
