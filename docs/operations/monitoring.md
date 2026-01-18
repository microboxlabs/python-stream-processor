# Monitoring

> TODO: Prometheus metrics

## Metrics Endpoint

```bash
METRICS_ENABLED=true
METRICS_PORT=9090
```

Endpoint: `http://localhost:9090/metrics`

## Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frames_received_total` | Counter | Frames received per device |
| `segments_generated_total` | Counter | Segments generated per device |
| `segments_deleted_total` | Counter | Segments deleted during cleanup |
| `active_devices_gauge` | Gauge | Currently active devices |
| `ffmpeg_duration_histogram` | Histogram | FFmpeg processing time |
| `cleanup_duration_histogram` | Histogram | Cleanup cycle duration |

## Grafana Dashboard

TODO: Dashboard JSON

## Code Reference

- `src/stream_processor/utils/metrics.py`
