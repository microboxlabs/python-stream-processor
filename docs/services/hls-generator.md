# HLS Generator

> TODO: FFmpeg segment generation

## Overview

Generates HLS video segments from accumulated frames using FFmpeg.

## Segment Generation

- Input: List of frame images (JPG/PNG)
- Output: MPEG-TS segment (.ts file)
- Codec: H.264 (libx264)
- Preset: ultrafast, zerolatency

## Playlist Management

- Rolling window of last 24 hours
- Atomic writes to prevent partial reads
- Segments referenced by number: `seg_000001.ts`

## Code Reference

- `src/stream_processor/service/hls_generator.py`
