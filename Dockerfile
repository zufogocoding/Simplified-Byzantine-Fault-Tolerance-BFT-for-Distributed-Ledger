FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Expose port (will be overridden per node)
EXPOSE 5000

# Entry point: run node.py with node_id as argument
ENTRYPOINT ["python", "node.py"]
CMD ["0"]
