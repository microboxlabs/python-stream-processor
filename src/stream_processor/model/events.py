"""
Event models for stream processing.
"""

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LocationData(BaseModel):
    """GPS location data."""

    lat: float = Field(description="Latitude")
    lon: float = Field(description="Longitude")


class FrameEvent(BaseModel):
    """Frame event from Pulsar topic."""

    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventId", description="Unique event identifier")
    client_id: str = Field(alias="clientId", description="Client identifier (from JWT)")
    device_id: str = Field(alias="deviceId", description="Device identifier")
    timestamp: datetime = Field(description="Frame capture timestamp")
    request_timestamp: datetime | None = Field(
        default=None,
        alias="requestTimestamp",
        description="HTTP request timestamp (Unix epoch seconds)",
    )
    frame_path: str = Field(alias="framePath", description="Path to frame image on shared storage")
    request_id: str = Field(alias="requestId", description="Request identifier for tracking")
    secondary_key: str | None = Field(
        default=None, alias="secondaryKey", description="Secondary index key"
    )
    location: LocationData | None = None

    @field_validator("request_timestamp", mode="before")
    @classmethod
    def parse_request_timestamp(cls, v: int | float | datetime | None) -> datetime | None:
        """Convert Unix epoch seconds to datetime if needed."""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        # Convert Unix epoch seconds to datetime
        return datetime.fromtimestamp(v, tz=UTC)


class DeviceState(BaseModel):
    """State tracking for a device's stream processing."""

    client_id: str = Field(description="Client identifier")
    device_id: str = Field(description="Device identifier")
    frame_count: int = Field(default=0, description="Accumulated frames since last segment")
    last_frame_time: datetime | None = Field(default=None, description="Last frame timestamp")
    last_segment_time: datetime | None = Field(
        default=None, description="Last segment generation time"
    )
    current_segment_number: int = Field(default=0, description="Current segment number")
    pending_frames: list[str] = Field(default_factory=list, description="Paths to pending frames")
    is_active: bool = Field(default=True, description="Whether device is actively streaming")

    @property
    def state_key(self) -> str:
        """Unique key for this client/device combination."""
        return f"{self.client_id}:{self.device_id}"

    def add_frame(self, frame_path: str, timestamp: datetime) -> None:
        """Add a frame to pending frames."""
        self.pending_frames.append(frame_path)
        self.frame_count = len(self.pending_frames)
        self.last_frame_time = timestamp

    def clear_pending_frames(self) -> list[str]:
        """Clear and return pending frames after segment generation."""
        frames = self.pending_frames.copy()
        self.pending_frames = []
        self.frame_count = 0
        self.current_segment_number += 1
        self.last_segment_time = datetime.now(UTC)
        return frames

    def should_generate_segment(self, frames_per_segment: int, max_wait_seconds: int = 60) -> bool:
        """
        Determine if a segment should be generated.

        Triggers on:
        1. Accumulated frames >= frames_per_segment
        2. OR time since last segment > max_wait_seconds (with at least 1 frame)
        """
        if self.frame_count >= frames_per_segment:
            return True

        if self.frame_count > 0 and self.last_segment_time:
            elapsed = (datetime.now(UTC) - self.last_segment_time).total_seconds()
            if elapsed > max_wait_seconds:
                return True

        return False
