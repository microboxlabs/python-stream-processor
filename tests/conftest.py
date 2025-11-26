"""Pytest configuration and fixtures."""

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest


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
