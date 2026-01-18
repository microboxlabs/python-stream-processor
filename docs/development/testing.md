# Testing

> TODO: Running & writing tests

## Running Tests

```bash
# All tests
uv run pytest

# With verbose output
uv run pytest -v

# Specific file
uv run pytest tests/test_redis_playlist_store.py

# With coverage
uv run pytest --cov=stream_processor --cov-report=term-missing
```

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── test_models.py           # Data model tests
└── test_redis_playlist_store.py  # Redis store tests
```

## Writing Tests

- Use pytest fixtures for setup
- Use `fakeredis` for Redis mocking
- Use `pytest-asyncio` for async tests

## Fixtures

Available in `conftest.py`:
- `fake_redis` - FakeRedis instance
- `playlist_store` - RedisPlaylistStore with fake Redis
- `sample_client_id` / `sample_device_id` - Test identifiers
