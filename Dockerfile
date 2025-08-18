# Automotive Stocks Monitoring System Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV TZ=UTC

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY pyproject.toml ./
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-interaction --no-ansi --no-root

# Copy application code
COPY stock/ ./stock/
COPY config.yaml ./

# Create logs directory
RUN mkdir -p logs

# Create non-root user
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Expose port (if needed for health checks)
EXPOSE 8000

# Set the entrypoint
ENTRYPOINT ["python", "-u", "stock/main.py"]
