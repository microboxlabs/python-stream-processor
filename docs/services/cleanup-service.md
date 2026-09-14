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

The interval must stay well below the retention window: a segment lives up to
`retention_hours + interval` before deletion. At the defaults that is 25 hours.
The setting must be greater than zero; the service would otherwise scan
continuously.

`PROCESSING_CLEANUP_INTERVAL_SECONDS` governs the long-running service started
by `main.py`, which is what the image runs by default. `stream-processor
cleanup` performs one pass and exits, for running under an external scheduler —
there the schedule sets the frequency and this setting does nothing.

## Cost on GCS

One cycle performs one device scan, shared between segment cleanup and frame
cleanup. The scan costs one Class A operation per prefix page
(`GcsStorageBackend.list_all_devices`) — `1 + N_clients` while each level fits
in a page, more once a level exceeds 1000 prefixes. It does not depend on how
many objects are stored. Per-device segment and frame listings are separate.

Lowering the interval multiplies that cost linearly. `scripts/gcs_scan_cost.py`
measures the real per-scan cost against a bucket.

## Lifecycle backstop

`scripts/gcs_lifecycle.json` is an unapplied bucket lifecycle rule that deletes
`.ts` at 8 days and frames at 2 days, catching objects the services fail to
delete. GCS `matchesPrefix` matches from the start of the object name, and every
object here starts with `client_ids/`, so the rules cannot be scoped to
`hls/segments/` or to archives — they match by suffix across the bucket.

That makes the ages a hard ceiling on retention. Before applying the file, and
before raising either retention setting, check:

| Setting | Must stay below |
|---|---|
| `PROCESSING_RETENTION_HOURS` + interval | 48 hours (the frame rule) |
| `ARCHIVE_RETENTION_DAYS` | 8 days (the `.ts` rule) |

Raising a retention setting past its lifecycle age means the bucket deletes data
the services still consider live. Raise the age in the JSON first.

## Code Reference

- `src/stream_processor/service/cleanup_service.py`
