# Architecture

> TODO: System design & data flow

## Overview

```
Frame Event (Pulsar) → Consumer → HLS Generator (FFmpeg) → Storage (GCS/Filesystem)
                                         ↓
                                   playlist.m3u8
```

## Components

- **Consumer**: Pulsar Key_Shared subscription
- **HLS Generator**: FFmpeg segment creation
- **Storage Backend**: Filesystem or GCS
- **Redis Stores**: Session tracking, playlist metadata
- **Cleanup Service**: Retention management
- **Offline Checker**: Archive creation

## Data Flow

TODO: Detailed sequence diagram

## Directory Structure

```
client_ids/{client_id}/device_id/{device_id}/
├── frames/           # Source frames
└── hls/
    ├── segments/     # .ts files
    └── playlist.m3u8
```
