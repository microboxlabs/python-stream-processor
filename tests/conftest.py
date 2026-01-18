"""Pytest configuration and fixtures."""

import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import fakeredis.aioredis

from stream_processor.service.redis_playlist_store import RedisPlaylistStore


@pytest.fixture
def sample_frame_event_data():
    """Sample frame event data for testing."""
    return {
        "eventId": "test-event-001",
        "deviceId": "device-001",
        "timestamp": "2025-11-25T10:30:00Z",
        "framePath": "/streamhub/frames/device-001/1732528200000.jpg",
        "metadata": {
            "licensePlate": "ABC123",
            "location": {"lat": -33.4489, "lon": -70.6693},
        },
    }


@pytest.fixture
def temp_storage_path(tmp_path):
    """Create temporary storage directories for testing."""
    frames_path = tmp_path / "frames"
    hls_path = tmp_path / "hls"
    frames_path.mkdir()
    hls_path.mkdir()

    return {
        "base": str(tmp_path),
        "frames": str(frames_path),
        "hls": str(hls_path),
    }


@pytest.fixture
def fake_redis():
    """Create a fake Redis client for testing."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
async def playlist_store(fake_redis):
    """Create a RedisPlaylistStore with a fake Redis client for testing."""
    store = RedisPlaylistStore()
    # Inject the fake Redis client directly
    store._client = fake_redis
    yield store
    # Cleanup
    await fake_redis.flushall()


@pytest.fixture
def sample_client_id():
    """Sample client ID for testing."""
    return "test-client-001"


@pytest.fixture
def sample_device_id():
    """Sample device ID for testing."""
    return "test-device-001"
