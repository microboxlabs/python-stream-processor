# Offline Checker

> TODO: Offline detection & archiving

## Overview

Detects when devices go offline and triggers archive creation.

## Detection Logic

- Device is offline if no frames for `ARCHIVE_OFFLINE_THRESHOLD_SECONDS`
- Sessions exceeding `ARCHIVE_MAX_SESSION_DURATION_SECONDS` are auto-broken

## Archive Creation

- Copies segments to archive location
- Generates VOD playlist
- Records metadata in PostgreSQL

## Code Reference

- `src/stream_processor/service/offline_checker.py`
- `src/stream_processor/service/archive_service.py`
