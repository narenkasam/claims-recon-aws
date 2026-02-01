# Lightweight python base
FROM python:3.12-slim

# Prevent python from writing .pyc files and buffering logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install OS deps (optional but useful for troubleshooting)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# App directory
WORKDIR /app

# Install python deps first (layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy code
COPY run.py /app/run.py

# Default command (ECS will override args, but keep a sensible default)
ENTRYPOINT []
CMD ["python", "/app/run.py"]

