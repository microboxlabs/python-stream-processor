# Multi-stage build for Python Stream Processor
# Stage 1: Builder stage with all build dependencies
FROM python:3.12-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    make \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml README.md ./
COPY src ./src
COPY main.py ./

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv --version

# Add uv to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Install Python dependencies using uv
RUN /root/.local/bin/uv sync --frozen || /root/.local/bin/uv sync

# Stage 2: Runtime image with minimal dependencies
FROM python:3.12-slim as runtime

# Install only runtime dependencies (FFmpeg)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy uv binary from builder
COPY --from=builder /root/.local/bin/uv /root/.local/bin/uv

# Copy the entire application and virtual environment from builder
COPY --from=builder /app /app

# Add uv to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Environment variables (can be overridden at runtime)
ENV PULSAR_SERVICE_URL=pulsar://pulsar:6650
ENV PULSAR_TOPIC=persistent://streamhub/stream/frames
ENV PULSAR_SUBSCRIPTION=stream-processor

# Storage: {base_path}/client_ids/{client_id}/device_id/{device_id}/frames|hls/
ENV STORAGE_BASE_PATH=/mnt/streamhub/streams

ENV PROCESSING_MAX_WORKERS=50
ENV PROCESSING_SEGMENT_DURATION_SECONDS=30
ENV PROCESSING_FRAMES_PER_SEGMENT=6
ENV PROCESSING_RETENTION_HOURS=24

ENV METRICS_PORT=9090
ENV METRICS_ENABLED=true

# Create base storage directory
RUN mkdir -p /mnt/streamhub/streams

# Expose metrics port
EXPOSE 9090

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "python main.py" || exit 1

# Run the application
CMD ["uv", "run", "main.py"]

