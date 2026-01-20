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

# Redis Playlist Metadata (optional, requires REDIS_ENABLED=true)
# When enabled, segment metadata is stored in Redis for on-the-fly playlist generation
REDIS_PLAYLIST_ENABLED=false

# Metrics
METRICS_PORT=9090
```

## CLI Commands

The stream processor provides several commands for different deployment scenarios:

### Consumer (Main Service)

Processes frames from Pulsar and generates HLS segments:

```bash
# Run the Pulsar consumer (default command)
uv run python -m stream_processor.main consumer

# Disable Redis session tracking (use in-memory only)
uv run python -m stream_processor.main consumer --no-redis
```

### Offline Checker

Detects offline devices and creates deferred transmission archives:

```bash
# Run continuously (default) - checks every 10 seconds
uv run python -m stream_processor.main offline-checker --continuous

# Run once and exit (for Kubernetes CronJob)
uv run python -m stream_processor.main offline-checker --once

# Custom check interval (in seconds)
uv run python -m stream_processor.main offline-checker --interval 30
```

### Segment Cleanup

Cleans up old HLS segments and frames beyond the retention window (24h default):

```bash
# Run once (for Kubernetes CronJob)
uv run python -m stream_processor.main cleanup
```

### Archive Cleanup

Cleans up expired deferred transmission archives (7 days default):

```bash
# Run once (for Kubernetes CronJob)
uv run python -m stream_processor.main archive-cleanup
```

### Device Reset

Resets all data for a specific device (useful for troubleshooting and testing):

```bash
# Preview what would be deleted (dry run)
uv run python -m stream_processor.main reset-device -c CLIENT_ID -d DEVICE_ID --dry-run

# Actually delete all device data
uv run python -m stream_processor.main reset-device -c CLIENT_ID -d DEVICE_ID

# Skip Redis cleanup (only reset storage)
uv run python -m stream_processor.main reset-device -c CLIENT_ID -d DEVICE_ID --skip-redis

# Skip storage cleanup (only reset Redis)
uv run python -m stream_processor.main reset-device -c CLIENT_ID -d DEVICE_ID --skip-storage
```

**What gets deleted:**
- **Redis**: Playlist segments (`hls:segments:{clientId}:{deviceId}`), session data (`stream:session:{clientId}:{deviceId}`)
- **Storage**: Frames (`frames/*.jpg`), HLS segments (`hls/segments/seg_*.ts`), playlist (`hls/playlist.m3u8`)

### Help

```bash
# Show all available commands
uv run python -m stream_processor.main --help
```

## Quick Start

1. **Configure environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your Pulsar and storage settings
   ```

2. **Run the consumer** (main service):
   ```bash
   uv run python -m stream_processor.main consumer
   ```

3. **Run the offline checker** (separate process):
   ```bash
   uv run python -m stream_processor.main offline-checker
   ```

### Legacy Entry Point

For backwards compatibility, `main.py` in the project root runs both consumer and cleanup service together:

```bash
uv run main.py
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

Generated HLS streams are compatible with all major browsers:
- **Safari**: Native support
- **Chrome 142+**: Native support (January 2025)
- **Edge 142+**: Native support (Chromium-based)
- **Firefox**: Via hls.js library (native support planned)

> **Note**: Chrome 142 and newer now play `.m3u8` streams natively without requiring hls.js. For older browsers, use [hls.js](https://github.com/video-dev/hls.js/) as a fallback.

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

# Run consumer
docker run -d \
  --name stream-processor \
  -e PULSAR_SERVICE_URL=pulsar://pulsar:6650 \
  -v /mnt/streamhub:/mnt/streamhub \
  stream-processor:latest \
  uv run python -m stream_processor.main consumer

# Run offline checker
docker run -d \
  --name offline-checker \
  -e REDIS_URL=redis://redis:6379 \
  -e REDIS_ENABLED=true \
  -v /mnt/streamhub:/mnt/streamhub \
  stream-processor:latest \
  uv run python -m stream_processor.main offline-checker --continuous
```

## Kubernetes Deployment

Recommended deployment architecture with single-responsibility components:

| Component | Type | Command |
|-----------|------|---------|
| Consumer | Deployment | `consumer` |
| Offline Checker | Deployment | `offline-checker --continuous` |
| Segment Cleanup | CronJob (*/5 * * * *) | `cleanup` |
| Archive Cleanup | CronJob (0 * * * *) | `archive-cleanup` |

Example CronJob for segment cleanup:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: stream-processor-cleanup
spec:
  schedule: "*/5 * * * *"  # Every 5 minutes
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: cleanup
            image: stream-processor:latest
            command: ["uv", "run", "python", "-m", "stream_processor.main", "cleanup"]
          restartPolicy: OnFailure
```

Example CronJob for archive cleanup:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: stream-processor-archive-cleanup
spec:
  schedule: "0 * * * *"  # Every hour
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: archive-cleanup
            image: stream-processor:latest
            command: ["uv", "run", "python", "-m", "stream_processor.main", "archive-cleanup"]
          restartPolicy: OnFailure
```

## Metrics

Prometheus metrics available at `http://localhost:9090/metrics`:

- `stream_processor_frames_received_total` - Total frames received
- `stream_processor_segments_generated_total` - Total HLS segments generated
- `stream_processor_active_devices` - Currently active devices
- `stream_processor_ffmpeg_duration_seconds` - FFmpeg processing time histogram

## License

Copyright © 2025 MicroboxLabs - MIT License

