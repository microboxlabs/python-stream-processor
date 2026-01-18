# Troubleshooting

> TODO: Common issues & debugging

## Common Issues

### Segments Not Being Generated

- Check FFmpeg is installed
- Verify frames exist in storage
- Check `PROCESSING_FRAMES_PER_SEGMENT` threshold

### Playlist References Missing Segments

- Cleanup may have deleted old segments
- Enable `REDIS_PLAYLIST_ENABLED` for dynamic playlists

### High Memory Usage

- Reduce `PROCESSING_MAX_WORKERS`
- Check for frame accumulation issues

### Redis Connection Errors

- Verify `REDIS_URL` is correct
- Check Redis is running and accessible

## Debug Logging

```bash
export LOG_LEVEL=DEBUG
stream-processor
```

## Health Checks

TODO: Health check endpoints
