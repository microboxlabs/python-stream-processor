"""
Event models for stream processing.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class LocationData(BaseModel):
    """GPS location data."""

    lat: float = Field(description="Latitude")
    lon: float = Field(description="Longitude")


class EventMetadata(BaseModel):
    """Event metadata."""

    license_plate: Optional[str] = Field(default=None, alias="licensePlate")
    location: Optional[LocationData] = None


class FrameEvent(BaseModel):
    """Frame event from Pulsar topic."""

    event_id: str = Field(alias="eventId", description="Unique event identifier")
    device_id: str = Field(alias="deviceId", description="Device identifier")
    timestamp: datetime = Field(description="Frame capture timestamp")
    frame_path: str = Field(alias="framePath", description="Path to frame image on shared storage")
    metadata: Optional[EventMetadata] = None

    class Config:
        populate_by_name = True


class DeviceState(BaseModel):
    """State tracking for a device's stream processing."""

    device_id: str = Field(description="Device identifier")
    frame_count: int = Field(default=0, description="Accumulated frames since last segment")
    last_frame_time: Optional[datetime] = Field(default=None, description="Last frame timestamp")
    last_segment_time: Optional[datetime] = Field(
        default=None, description="Last segment generation time"
    )
    current_segment_number: int = Field(default=0, description="Current segment number")
    pending_frames: list[str] = Field(
        default_factory=list, description="Paths to pending frames"
    )
    is_active: bool = Field(default=True, description="Whether device is actively streaming")

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
        self.last_segment_time = datetime.utcnow()
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
            elapsed = (datetime.utcnow() - self.last_segment_time).total_seconds()
            if elapsed > max_wait_seconds:
                return True

        return False

