# Cleanup Service

## Overview

Removes old HLS segments and source frames beyond the retention window.

## What Gets Deleted

- HLS segments older than `PROCESSING_RETENTION_HOURS`
- Source frames older than retention period
- Redis playlist metadata (if enabled)
- Stale temp files from the GCS backend (older than 10 minutes)

## Schedule

| Setting | Default | Effect |
|---|---|---|
| `PROCESSING_RETENTION_HOURS` | 24 | Age at which a segment or frame is deleted |
| `PROCESSING_CLEANUP_INTERVAL_SECONDS` | 3600 | Seconds between cycles |

Can be run as one-shot via `stream-processor cleanup`.

The interval must stay well below the retention window: a segment lives up to
`retention_hours + interval` before deletion. At the defaults that is 25 hours.

## Cost on GCS

One cycle performs one device scan, shared between segment cleanup and frame
cleanup. The scan costs `1 + N_clients` Class A operations
(`GcsStorageBackend.list_all_devices`), independent of how many objects are
stored. Per-device segment and frame listings are separate.

Lowering the interval multiplies that cost linearly. `scripts/gcs_scan_cost.py`
measures the real per-scan cost against a bucket.

## Code Reference

- `src/stream_processor/service/cleanup_service.py`
