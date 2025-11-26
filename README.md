# Stream Processor

Pulsar consumer microservice for HLS live stream generation with FFmpeg - processes device frames into 24h rolling video streams.

## Overview

This service consumes frame events from Apache Pulsar and generates HLS (HTTP Live Streaming) video segments for real-time playback. Each device stream maintains a 24-hour rolling window of video content.

## Features

- **Pulsar Integration** via native Python client (Key_Shared subscription for ordering)
- **HLS Generation** with FFmpeg (H.264 codec, browser-compatible)
- **24-Hour Rolling Window** with automatic segment cleanup
- **Horizontal Scaling** via Pulsar's Key_Shared subscription model
- **Worker Pool** for concurrent FFmpeg processing (50 workers default)
- **Device State Management** with Redis (optional) or in-memory
- **Prometheus Metrics** for monitoring
- **Structured Logging** with structlog

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     STREAM PROCESSOR                             │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Pulsar Consumer (Key_Shared by deviceId)                  │ │
│  │  - Topic: persistent://streamhub/v1/frames                 │ │
│  │  - Subscription: stream-processor                          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                       │
│                          ▼                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Frame Accumulator (per device)                            │ │
│  │  - Collects frames until segment threshold                 │ │
│  │  - Triggers segment generation every 30s or N frames       │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                       │
│                          ▼                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  FFmpeg Worker Pool (ThreadPoolExecutor)                   │ │
│  │  - MAX_WORKERS concurrent FFmpeg processes                 │ │
│  │  - Generates HLS segments (.ts) + playlist (.m3u8)         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                          │                                       │
│                          ▼                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Cleanup Service                                           │ │
│  │  - Removes segments older than 24 hours                    │ │
│  │  - Runs every 5 minutes                                    │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SHARED FILESYSTEM                             │
│  /storage/streams/                                               │
│  └── client_ids/{clientId}/                                      │
│      └── device_id/{deviceId}/                                   │
│          ├── frames/                      # Source frames        │
│          │   └── {eventId}.jpg                                   │
│          └── hls/                         # Generated HLS        │
│              ├── playlist.m3u8            # Rolling playlist     │
│              └── segments/                # Video segments       │
│                  └── seg_{NNNNNN}.ts                             │
└─────────────────────────────────────────────────────────────────┘
```

## Requirements

- Python 3.12+
- FFmpeg installed on system
- Apache Pulsar (broker)
- Redis (optional, for distributed state)

## Installation

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install with dev dependencies
uv sync --extra dev
```

## Configuration

Create a `.env` file:

```env
# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://localhost:6650
PULSAR_TOPIC=persistent://streamhub/stream/frames
PULSAR_SUBSCRIPTION=stream-processor

# Storage Configuration
# Directory structure: {base_path}/client_ids/{client_id}/device_id/{device_id}/frames|hls/
STORAGE_BASE_PATH=/storage/streams

# Processing Configuration
PROCESSING_MAX_WORKERS=50
PROCESSING_SEGMENT_DURATION_SECONDS=30
PROCESSING_FRAMES_PER_SEGMENT=6
PROCESSING_RETENTION_HOURS=24

# Redis Configuration (optional)
REDIS_URL=redis://localhost:6379
REDIS_ENABLED=false

# Metrics
METRICS_PORT=9090
```

## Quick Start

1. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your Pulsar and storage settings
   ```

2. **Run the processor**:
   ```bash
   # Using uv (recommended)
   uv run main.py

   # Or using Python directly
   python main.py
   ```

## Scaling

The service scales horizontally via Pulsar's **Key_Shared** subscription:

| Scale | Replicas | Workers/Replica | Devices/Replica |
|-------|----------|-----------------|-----------------|
| 1,000 devices | 4 | 50 | ~250 |
| 5,000 devices | 10 | 50 | ~500 |
| 10,000 devices | 20 | 50 | ~500 |

Deploy multiple instances (K8s replicas) and Pulsar will distribute devices across them while maintaining ordering per device.

## HLS Output

Generated HLS streams are compatible with:
- **Safari**: Native support
- **Chrome/Firefox/Edge**: Via hls.js library

Example playlist (`playlist.m3u8`):
```m3u8
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:30
#EXT-X-MEDIA-SEQUENCE:100
#EXTINF:30.0,
segments/seg_000100.ts
#EXTINF:30.0,
segments/seg_000101.ts
...
```

## Message Schema

Expected Pulsar message format:

```json
{
  "eventId": "uuid-v4",
  "clientId": "client-abc",
  "deviceId": "device-001",
  "timestamp": "2025-11-25T10:30:00Z",
  "framePath": "/storage/streams/client_ids/client-abc/device_id/device-001/frames/uuid-v4.jpg",
  "requestId": "request-uuid",
  "secondaryKey": "optional-secondary-key",
  "location": {
    "lat": -33.4489,
    "lon": -70.6693
  }
}
```

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run black .

# Lint code
uv run ruff check .

# Type check
uv run mypy src/
```

## Docker

```bash
# Build image
docker build -t stream-processor:latest .

# Run container
docker run -d \
  --name stream-processor \
  -e PULSAR_SERVICE_URL=pulsar://pulsar:6650 \
  -v /mnt/streamhub:/mnt/streamhub \
  stream-processor:latest
```

## Metrics

Prometheus metrics available at `http://localhost:9090/metrics`:

- `stream_processor_frames_received_total` - Total frames received
- `stream_processor_segments_generated_total` - Total HLS segments generated
- `stream_processor_active_devices` - Currently active devices
- `stream_processor_ffmpeg_duration_seconds` - FFmpeg processing time histogram

## License

Copyright © 2025 MicroboxLabs - MIT License

