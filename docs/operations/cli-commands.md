# CLI Commands

> TODO: Available commands

## Main Consumer

```bash
stream-processor
```

Starts the Pulsar consumer and processes frames.

## Cleanup (One-shot)

```bash
stream-processor cleanup
```

Runs a single cleanup cycle. Useful for CronJob deployments.

## Offline Checker

```bash
# Continuous mode
stream-processor offline-checker --continuous

# One-shot mode
stream-processor offline-checker --once
```

## Archive Cleanup

```bash
stream-processor archive-cleanup
```

Removes expired archives.

## Code Reference

- `src/stream_processor/main.py`
