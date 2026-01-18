# Getting Started

> TODO: Quick start guide

## Prerequisites

- Python 3.12+
- FFmpeg
- Pulsar broker
- Redis (optional)

## Installation

```bash
uv sync
```

## Minimal Configuration

```bash
export PULSAR_SERVICE_URL=pulsar://localhost:6650
export PULSAR_TOPIC=persistent://streamhub/stream/frames
export STORAGE_TYPE=filesystem
export STORAGE_BASE_PATH=/storage/streams
```

## Running

```bash
stream-processor
```

## Next Steps

- [Configuration](configuration.md) - All environment variables
- [Architecture](architecture.md) - How it works
