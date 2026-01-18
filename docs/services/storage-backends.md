# Storage Backends

> TODO: Filesystem vs GCS

## Overview

Supports local filesystem and Google Cloud Storage backends.

## Filesystem

```bash
STORAGE_TYPE=filesystem
STORAGE_BASE_PATH=/storage/streams
```

## Google Cloud Storage

```bash
STORAGE_TYPE=gcs
STORAGE_GCS_BUCKET=my-bucket
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Directory Structure

Both backends use the same logical structure:

```
client_ids/{client_id}/device_id/{device_id}/
├── frames/
├── hls/
│   ├── segments/
│   └── playlist.m3u8
└── archives/
```

## Code Reference

- `src/stream_processor/service/storage_backend.py`
