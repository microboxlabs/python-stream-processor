# Redis Stores

> TODO: Session store & playlist store

## Overview

Two Redis-backed stores for distributed state management.

## Session Store

Tracks active streaming sessions for offline detection.

```
Key: stream:session:{client_id}:{device_id}
Value: JSON with session_id, timestamps, segment numbers
```

## Playlist Store

Stores segment metadata for dynamic playlist generation.

```
Key: hls:segments:{client_id}:{device_id}
Type: Sorted Set (ZSET)
Score: Unix timestamp
Member: Segment number
```

## Configuration

```bash
REDIS_ENABLED=true
REDIS_PLAYLIST_ENABLED=true
REDIS_URL=redis://localhost:6379
```

## Code Reference

- `src/stream_processor/service/redis_session_store.py`
- `src/stream_processor/service/redis_playlist_store.py`
