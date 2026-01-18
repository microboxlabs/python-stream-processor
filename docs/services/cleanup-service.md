# Cleanup Service

> TODO: Retention & cleanup logic

## Overview

Removes old HLS segments and source frames beyond the retention window.

## What Gets Deleted

- HLS segments older than `PROCESSING_RETENTION_HOURS`
- Source frames older than retention period
- Redis playlist metadata (if enabled)

## Schedule

- Runs every 5 minutes
- Can be run as one-shot via `stream-processor cleanup`

## Code Reference

- `src/stream_processor/service/cleanup_service.py`
