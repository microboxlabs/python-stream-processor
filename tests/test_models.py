"""Tests for data models."""

from datetime import datetime, timedelta

import pytest

from stream_processor.model.events import FrameEvent, DeviceState


class TestFrameEvent:
    """Tests for FrameEvent model."""

    def test_parse_from_dict(self):
        """Test parsing FrameEvent from dictionary."""
        data = {
            "eventId": "test-event-001",
            "deviceId": "device-001",
            "timestamp": "2025-11-25T10:30:00Z",
            "framePath": "/streamhub/frames/device-001/1732528200000.jpg",
            "metadata": {
                "licensePlate": "ABC123",
                "location": {"lat": -33.4489, "lon": -70.6693},
            },
        }

        event = FrameEvent.model_validate(data)

        assert event.event_id == "test-event-001"
        assert event.device_id == "device-001"
        assert event.frame_path == "/streamhub/frames/device-001/1732528200000.jpg"
        assert event.metadata.license_plate == "ABC123"
        assert event.metadata.location.lat == -33.4489

    def test_parse_minimal_event(self):
        """Test parsing minimal FrameEvent without optional fields."""
        data = {
            "eventId": "test-event-002",
            "deviceId": "device-002",
            "timestamp": "2025-11-25T10:30:00Z",
            "framePath": "/streamhub/frames/device-002/1732528200000.jpg",
        }

        event = FrameEvent.model_validate(data)

        assert event.event_id == "test-event-002"
        assert event.metadata is None


class TestDeviceState:
    """Tests for DeviceState model."""

    def test_add_frame(self):
        """Test adding frames to device state."""
        state = DeviceState(device_id="test-device")
        now = datetime.utcnow()

        state.add_frame("/path/to/frame1.jpg", now)
        state.add_frame("/path/to/frame2.jpg", now)

        assert state.frame_count == 2
        assert len(state.pending_frames) == 2
        assert state.last_frame_time == now

    def test_clear_pending_frames(self):
        """Test clearing pending frames after segment generation."""
        state = DeviceState(device_id="test-device")
        now = datetime.utcnow()

        state.add_frame("/path/to/frame1.jpg", now)
        state.add_frame("/path/to/frame2.jpg", now)

        frames = state.clear_pending_frames()

        assert len(frames) == 2
        assert state.frame_count == 0
        assert len(state.pending_frames) == 0
        assert state.current_segment_number == 1

    def test_should_generate_segment_by_count(self):
        """Test segment generation trigger by frame count."""
        state = DeviceState(device_id="test-device")
        now = datetime.utcnow()

        # Add 5 frames (less than threshold of 6)
        for i in range(5):
            state.add_frame(f"/path/to/frame{i}.jpg", now)

        assert not state.should_generate_segment(frames_per_segment=6)

        # Add one more frame (reaches threshold)
        state.add_frame("/path/to/frame5.jpg", now)

        assert state.should_generate_segment(frames_per_segment=6)

    def test_should_generate_segment_by_time(self):
        """Test segment generation trigger by time threshold."""
        state = DeviceState(device_id="test-device")
        now = datetime.utcnow()

        # Set last segment time to 70 seconds ago
        state.last_segment_time = now - timedelta(seconds=70)
        state.add_frame("/path/to/frame0.jpg", now)

        # Should trigger due to time threshold (60s default)
        assert state.should_generate_segment(frames_per_segment=6, max_wait_seconds=60)

    def test_should_not_generate_empty_segment(self):
        """Test that empty segments are not generated."""
        state = DeviceState(device_id="test-device")
        state.last_segment_time = datetime.utcnow() - timedelta(seconds=120)

        # No frames, should not generate even if time threshold passed
        assert not state.should_generate_segment(frames_per_segment=6, max_wait_seconds=60)

