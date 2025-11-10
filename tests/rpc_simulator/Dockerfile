FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY mock_rpc.py .

# Expose port
EXPOSE 8545

# Run application
CMD ["python", "mock_rpc.py"]

