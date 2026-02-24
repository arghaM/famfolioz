FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy source and install package
COPY . .
RUN pip install --no-cache-dir -e .

# Data directory — mount a volume here for persistence
ENV FAMFOLIOZ_DATA_DIR=/data
RUN mkdir -p /data/backups

EXPOSE 5000

# Single worker + threads (SQLite doesn't support concurrent writers)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "2", "--timeout", "120", "cas_parser.webapp.app:app"]
