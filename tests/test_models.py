"""Tests for data models."""

from datetime import UTC, datetime, timedelta

from stream_processor.model.events import DeviceState, FrameEvent


class TestFrameEvent:
    """Tests for FrameEvent model."""

    def test_parse_from_dict(self):
        """Test parsing FrameEvent from dictionary."""
        data = {
            "eventId": "test-event-001",
            "clientId": "client-001",
            "deviceId": "device-001",
            "timestamp": "2025-11-25T10:30:00Z",
            "framePath": "/streamhub/frames/device-001/1732528200000.jpg",
            "requestId": "request-001",
            "secondaryKey": "ABC123",
            "location": {"lat": -33.4489, "lon": -70.6693},
        }

        event = FrameEvent.model_validate(data)

        assert event.event_id == "test-event-001"
        assert event.client_id == "client-001"
        assert event.device_id == "device-001"
        assert event.frame_path == "/streamhub/frames/device-001/1732528200000.jpg"
        assert event.request_id == "request-001"
        assert event.secondary_key == "ABC123"
        assert event.location is not None
        assert event.location.lat == -33.4489

    def test_parse_minimal_event(self):
        """Test parsing minimal FrameEvent without optional fields."""
        data = {
            "eventId": "test-event-002",
            "clientId": "client-002",
            "deviceId": "device-002",
            "timestamp": "2025-11-25T10:30:00Z",
            "framePath": "/streamhub/frames/device-002/1732528200000.jpg",
            "requestId": "request-002",
        }

        event = FrameEvent.model_validate(data)

        assert event.event_id == "test-event-002"
        assert event.client_id == "client-002"
        assert event.secondary_key is None
        assert event.location is None


class TestDeviceState:
    """Tests for DeviceState model."""

    def test_add_frame(self):
        """Test adding frames to device state."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        now = datetime.now(UTC)

        state.add_frame("/path/to/frame1.jpg", now)
        state.add_frame("/path/to/frame2.jpg", now)

        assert state.frame_count == 2
        assert len(state.pending_frames) == 2
        assert state.last_frame_time == now

    def test_clear_pending_frames(self):
        """Test clearing pending frames after segment generation."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        now = datetime.now(UTC)

        state.add_frame("/path/to/frame1.jpg", now)
        state.add_frame("/path/to/frame2.jpg", now)

        frames = state.clear_pending_frames()

        assert len(frames) == 2
        assert state.frame_count == 0
        assert len(state.pending_frames) == 0
        assert state.current_segment_number == 1

    def test_pending_first_frame_time_tracks_batch_start(self):
        """First-frame time is set on the first frame and held for the batch."""
        state = DeviceState(client_id="c", device_id="d")
        t0 = datetime.now(UTC)
        t1 = t0 + timedelta(seconds=1)

        assert state.pending_first_frame_time is None
        state.add_frame("/f0.jpg", t0)
        assert state.pending_first_frame_time == t0
        # Later frames in the same batch must not move the batch-start time.
        state.add_frame("/f1.jpg", t1)
        assert state.pending_first_frame_time == t0

    def test_pending_first_frame_time_resets_per_segment(self):
        """Clearing resets the batch-start time so the next batch tracks fresh."""
        state = DeviceState(client_id="c", device_id="d")
        t0 = datetime.now(UTC)
        t_next = t0 + timedelta(seconds=6)

        state.add_frame("/f0.jpg", t0)
        state.clear_pending_frames()
        assert state.pending_first_frame_time is None

        state.add_frame("/f6.jpg", t_next)
        assert state.pending_first_frame_time == t_next

    def test_should_generate_segment_by_count(self):
        """Test segment generation trigger by frame count."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        now = datetime.now(UTC)

        # Add 5 frames (less than threshold of 6)
        for i in range(5):
            state.add_frame(f"/path/to/frame{i}.jpg", now)

        assert not state.should_generate_segment(frames_per_segment=6)

        # Add one more frame (reaches threshold)
        state.add_frame("/path/to/frame5.jpg", now)

        assert state.should_generate_segment(frames_per_segment=6)

    def test_should_generate_segment_by_time(self):
        """Test segment generation trigger by time threshold."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        now = datetime.now(UTC)

        # Set last segment time to 70 seconds ago
        state.last_segment_time = now - timedelta(seconds=70)
        state.add_frame("/path/to/frame0.jpg", now)

        # Should trigger due to time threshold (60s default)
        assert state.should_generate_segment(frames_per_segment=6, max_wait_seconds=60)

    def test_should_not_generate_empty_segment(self):
        """Test that empty segments are not generated."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        state.last_segment_time = datetime.now(UTC) - timedelta(seconds=120)

        # No frames, should not generate even if time threshold passed
        assert not state.should_generate_segment(frames_per_segment=6, max_wait_seconds=60)

    def test_state_key(self):
        """Test state_key property."""
        state = DeviceState(client_id="test-client", device_id="test-device")
        assert state.state_key == "test-client:test-device"
