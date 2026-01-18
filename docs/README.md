# Stream Processor Documentation

> HLS live stream generation from device frames using Pulsar and FFmpeg

## Quick Links

- [Getting Started](getting-started.md)
- [Architecture](architecture.md)
- [Configuration](configuration.md)
- [Deployment](deployment.md)

## Services

- [Consumer](services/consumer.md) - Pulsar consumer & frame processing
- [HLS Generator](services/hls-generator.md) - FFmpeg segment generation
- [Cleanup Service](services/cleanup-service.md) - Retention & cleanup logic
- [Offline Checker](services/offline-checker.md) - Offline detection & archiving
- [Redis Stores](services/redis-stores.md) - Session store & playlist store
- [Storage Backends](services/storage-backends.md) - Filesystem vs GCS

## Features

- [Watermarking](features/watermarking.md) - Timestamp overlay
- [Archiving](features/archiving.md) - Deferred transmissions

## Operations

- [Monitoring](operations/monitoring.md) - Prometheus metrics
- [Troubleshooting](operations/troubleshooting.md) - Common issues
- [CLI Commands](operations/cli-commands.md) - Available commands

## Development

- [Contributing](development/contributing.md) - Dev setup & guidelines
- [Testing](development/testing.md) - Running & writing tests
