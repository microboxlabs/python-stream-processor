# Watermarking

> TODO: Timestamp overlay feature

## Overview

Adds timestamp watermarks to frames before segment generation.

## Configuration

```bash
WATERMARK_ENABLED=true
WATERMARK_POSITION=top_right
WATERMARK_FONT_SIZE=24
WATERMARK_FORMAT=%Y-%m-%d %H:%M:%S.%f
WATERMARK_TIMEZONE=America/Santiago
WATERMARK_SHOW_TIMEZONE=true
```

## Positions

- `top_right`
- `top_left`
- `bottom_right`
- `bottom_left`

## Code Reference

- `src/stream_processor/service/watermark_service.py`
