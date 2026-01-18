# Configuration

> TODO: All environment variables

## Pulsar (`PULSAR_*`)

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSAR_SERVICE_URL` | `pulsar://localhost:6650` | Broker URL |
| `PULSAR_TOPIC` | `persistent://streamhub/stream/frames` | Topic to consume |
| `PULSAR_SUBSCRIPTION` | `stream-processor` | Subscription name |
| `PULSAR_CONSUMER_NAME` | `stream-processor-consumer` | Consumer name |

## Storage (`STORAGE_*`)

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_TYPE` | `filesystem` | `filesystem` or `gcs` |
| `STORAGE_BASE_PATH` | `/storage/streams` | Base path for filesystem |
| `STORAGE_GCS_BUCKET` | - | GCS bucket name |
| `STORAGE_GCS_PROJECT_ID` | - | GCS project ID |

## Processing (`PROCESSING_*`)

| Variable | Default | Description |
|----------|---------|-------------|
| `PROCESSING_MAX_WORKERS` | `50` | FFmpeg worker threads |
| `PROCESSING_SEGMENT_DURATION_SECONDS` | `30` | Segment duration |
| `PROCESSING_FRAMES_PER_SEGMENT` | `6` | Frames per segment |
| `PROCESSING_RETENTION_HOURS` | `24` | Hours to retain |

## Redis (`REDIS_*`)

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `REDIS_ENABLED` | `false` | Enable Redis |
| `REDIS_PLAYLIST_ENABLED` | `false` | Enable playlist store |

## Archive (`ARCHIVE_*`)

TODO: Archive configuration

## Watermark (`WATERMARK_*`)

TODO: Watermark configuration

## Metrics (`METRICS_*`)

TODO: Metrics configuration
