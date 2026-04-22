FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required to build psycopg2 from source if
# the binary wheel is unavailable. Remove build tools after install to
# keep the image small.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for better Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PREFECT_HOME=/tmp/prefect

RUN mkdir -p /tmp/prefect /root/.prefect

CMD ["python", "flows/etl_flow.py"]