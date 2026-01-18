# Archiving

> TODO: Deferred transmissions

## Overview

Creates VOD archives when devices go offline, stored for later retrieval.

## Archive Structure

```
archives/{session_id}/
├── segments/
│   ├── seg_000100.ts
│   └── ...
└── playlist.m3u8
```

## Database Schema

Archives are tracked in PostgreSQL `deferred_transmissions` table.

## Retention

Archives are deleted after `ARCHIVE_RETENTION_DAYS` (default: 7 days).

## Code Reference

- `src/stream_processor/service/archive_service.py`
