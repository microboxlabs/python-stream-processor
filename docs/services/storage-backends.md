# Storage Backends

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

## Operation Costs (GCS)

Every GCS call is billed. Class A (listing, writing) is ~20x the price of
Class B (reading metadata or content); deletes are free. So are 4xx responses,
absent a website configuration, so acting on an object that turns out to be
missing costs nothing. The table counts the successful case.

| Method | Class A | Class B | Note |
|---|---|---|---|
| `list_all_devices` | 1 per prefix page | 0 | Walks two prefix levels with `delimiter="/"`; `1 + N_clients` while each level fits in a page |
| `list_files` | 1 per 1000 objects in the directory | 0 | Lists the whole directory, then filters by pattern client-side |
| `write_file` | 1 | 0 | |
| `read_file` | 0 | 1 | Downloads and catches `NotFound` |
| `get_file_info` | 0 | 1 | `get_blob()`, not `exists()` + `reload()` |
| `file_exists` | 0 | 1 | Prefer acting and catching `NotFound` |
| `delete_file` | 0 | 0 | Deletes and catches `NotFound` |

Two rules:

- Never call `exists()` before an operation that reports absence itself.
  `delete()`, `download_as_bytes()` and `get_blob()` all signal a missing
  object. `exists()` adds a billable round trip.
- Never list without a `delimiter` or a device-scoped prefix. A bare
  `list_blobs(prefix="client_ids/")` enumerates the whole bucket, so its cost
  tracks objects stored, not devices.

A page holds 1000 results, and GCS counts collapsed prefixes toward that limit
alongside objects. A client with more than 1000 devices costs more than one
operation to list.

`list_files` does not filter on the server. It lists every object directly under
the subpath and applies `fnmatch` locally, so a pattern matching two files in a
directory of 3000 still costs three operations. Cost follows the directory's
size, not the match count.

`scripts/gcs_scan_cost.py` measures the per-scan cost against a live bucket.

## Code Reference

- `src/stream_processor/service/storage_backend.py`
