"""Tests for SessionData capture-time bounds (recording time range vs wall-clock)."""

import json

from stream_processor.service.redis_session_store import SessionData

# 17:54 -> 17:57 wall-clock (3 min processing window).
_BASE = {
    "client_id": "c",
    "device_id": "d",
    "session_id": "s",
    "started_at": "2026-06-06T17:54:00+00:00",
    "last_frame_at": "2026-06-06T17:57:00+00:00",
    "first_segment_number": 0,
    "last_segment_number": 5,
    "frame_count": 30,
}


def _sess(**kw):
    return SessionData(**{**_BASE, **kw})


class TestSessionDataCaptureTime:
    def test_falls_back_to_wallclock_when_no_capture(self):
        s = _sess()
        assert s.captured_started_dt == s.started_at_dt
        assert s.captured_ended_dt == s.last_frame_at_dt
        assert s.captured_duration_seconds == s.duration_seconds == 180

    def test_uses_capture_times_when_present(self):
        # Frames captured 11:50 -> 12:05 (15 min) but processed in the 3-min window.
        s = _sess(
            first_frame_captured_at="2026-06-06T11:50:00+00:00",
            last_frame_captured_at="2026-06-06T12:05:00+00:00",
        )
        # Display duration reflects the real capture span...
        assert s.captured_duration_seconds == 15 * 60
        # ...while wall-clock duration (offline detection / retention) is unchanged.
        assert s.duration_seconds == 180

    def test_from_json_back_compatible_without_capture_fields(self):
        old = json.dumps(_BASE)  # no capture fields (old producer)
        s = SessionData.from_json(old)
        assert s.first_frame_captured_at is None
        assert s.captured_duration_seconds == 180  # wall-clock fallback

    def test_from_json_ignores_unknown_keys(self):
        future = json.dumps({**_BASE, "some_future_field": 123})
        s = SessionData.from_json(future)
        assert s.session_id == "s"

    def test_to_json_round_trip_preserves_capture_fields(self):
        s = _sess(
            first_frame_captured_at="2026-06-06T11:50:00+00:00",
            last_frame_captured_at="2026-06-06T12:05:00+00:00",
        )
        back = SessionData.from_json(s.to_json())
        assert back.first_frame_captured_at == "2026-06-06T11:50:00+00:00"
        assert back.captured_duration_seconds == 15 * 60
