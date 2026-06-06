"""
Device Session Tracker

Tracks active streaming sessions and detects offline transitions
for triggering archive creation.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DeviceSession:
    """Tracks a single streaming session for a device."""

    client_id: str
    device_id: str
    session_id: str
    started_at: datetime  # WALL-CLOCK (offline detection, retention)
    last_frame_at: datetime  # WALL-CLOCK (offline detection, retention)
    first_segment_number: int
    last_segment_number: int = 0
    frame_count: int = 0
    # Capture-time bounds for the recording's displayed time range (fall back to
    # wall-clock when unavailable).
    captured_started_at: datetime | None = None
    captured_ended_at: datetime | None = None

    @property
    def state_key(self) -> str:
        """Get the state key for this device."""
        return f"{self.client_id}:{self.device_id}"

    @property
    def duration_seconds(self) -> int:
        """Get the session duration in seconds (wall-clock)."""
        return int((self.last_frame_at - self.started_at).total_seconds())

    @property
    def display_started_at(self) -> datetime:
        """Capture-time start for display (falls back to wall-clock)."""
        return self.captured_started_at or self.started_at

    @property
    def display_ended_at(self) -> datetime:
        """Capture-time end for display (falls back to wall-clock)."""
        return self.captured_ended_at or self.last_frame_at

    @property
    def captured_duration_seconds(self) -> int:
        """Real captured span in seconds (falls back to wall-clock duration)."""
        return int((self.display_ended_at - self.display_started_at).total_seconds())

    @property
    def segment_count(self) -> int:
        """Get the number of segments in this session."""
        return self.last_segment_number - self.first_segment_number + 1

    def update_activity(self) -> None:
        """Update session activity timestamp (called on frame receipt)."""
        self.last_frame_at = datetime.now(UTC)
        self.frame_count += 1

    def update_segment(self, segment_number: int) -> None:
        """Update session with new segment generation."""
        if segment_number > self.last_segment_number:
            self.last_segment_number = segment_number


class DeviceSessionTracker:
    """
    Tracks device streaming sessions and triggers archive creation
    when devices go offline.

    A device is considered offline when no frames are received for
    `offline_threshold_seconds` (default 60 seconds).
    """

    def __init__(
        self,
        on_session_ended: Callable[[DeviceSession], Awaitable[None]] | None = None,
    ):
        """
        Initialize the session tracker.

        Args:
            on_session_ended: Async callback when a session ends (device goes offline)
        """
        self.config = settings.archive
        self.sessions: dict[str, DeviceSession] = {}
        self.on_session_ended = on_session_ended
        self.running = False
        self._check_interval = 5  # Check every 5 seconds

    def update_activity(
        self,
        client_id: str,
        device_id: str,
    ) -> DeviceSession:
        """
        Update session activity timestamp when a frame is received.

        Called on every frame receipt to keep the session alive.
        Creates a new session if one doesn't exist.

        Args:
            client_id: Client identifier
            device_id: Device identifier

        Returns:
            The DeviceSession object
        """
        state_key = f"{client_id}:{device_id}"
        session = self.sessions.get(state_key)

        if session:
            session.update_activity()
        else:
            # Create new session on first frame
            session = DeviceSession(
                client_id=client_id,
                device_id=device_id,
                session_id=str(uuid.uuid4()),
                started_at=datetime.now(UTC),
                last_frame_at=datetime.now(UTC),
                first_segment_number=-1,  # Will be set when first segment is generated
                last_segment_number=-1,
                frame_count=1,
            )
            self.sessions[state_key] = session
            logger.info(
                f"New session started (on frame receipt): {state_key} session={session.session_id}"
            )

        return session

    def update_segment(
        self,
        client_id: str,
        device_id: str,
        segment_number: int,
    ) -> DeviceSession | None:
        """
        Update session with segment generation info.

        Called when a segment is generated for a device.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            segment_number: Current segment number

        Returns:
            The DeviceSession object if exists, None otherwise
        """
        state_key = f"{client_id}:{device_id}"
        session = self.sessions.get(state_key)

        if session:
            # Set first segment number if not yet set
            if session.first_segment_number < 0:
                session.first_segment_number = segment_number
                logger.info(f"Session first segment recorded: {state_key} segment={segment_number}")

            # Update last segment number
            session.update_segment(segment_number)
        else:
            logger.warning(
                f"No active session for segment update: {state_key} "
                f"(session should have been created on frame receipt)"
            )

        return session

    async def run(self) -> None:
        """
        Background task to detect offline devices.

        Runs continuously checking for devices that have gone offline
        (no frames for offline_threshold_seconds).
        """
        if not self.config.enabled:
            logger.info("Archive/deferred transmissions disabled")
            return

        logger.info("=" * 80)
        logger.info("Device Session Tracker Started")
        logger.info(f"Offline threshold: {self.config.offline_threshold_seconds}s")
        logger.info(f"Min session duration: {self.config.min_session_duration_seconds}s")
        logger.info(f"Retention: {self.config.retention_days} days")
        logger.info("=" * 80)

        self.running = True

        while self.running:
            await asyncio.sleep(self._check_interval)
            await self._check_offline_devices()

    async def stop(self) -> None:
        """
        Stop the tracker gracefully.

        Archives any remaining active sessions before stopping.
        """
        logger.info("Stopping session tracker...")
        self.running = False

        # Archive any remaining sessions
        for session in list(self.sessions.values()):
            logger.info(f"Archiving remaining session on shutdown: {session.state_key}")
            await self._end_session(session)

    async def _check_offline_devices(self) -> None:
        """Check for devices that have gone offline."""
        now = datetime.now(UTC)
        threshold = timedelta(seconds=self.config.offline_threshold_seconds)

        # Log tracker status for debugging
        if self.sessions:
            logger.debug(
                f"[SessionTracker] Checking {len(self.sessions)} active session(s), "
                f"threshold={self.config.offline_threshold_seconds}s"
            )
            for state_key, session in self.sessions.items():
                idle_seconds = (now - session.last_frame_at).total_seconds()
                logger.debug(
                    f"[SessionTracker] {state_key}: idle={idle_seconds:.1f}s, "
                    f"duration={session.duration_seconds}s, frames={session.frame_count}, "
                    f"segments={session.first_segment_number}-{session.last_segment_number}"
                )

        for state_key, session in list(self.sessions.items()):
            idle_time = now - session.last_frame_at
            if idle_time > threshold:
                logger.info(
                    f"Device offline detected: {state_key} "
                    f"(last frame {idle_time.total_seconds():.0f}s ago, "
                    f"session duration={session.duration_seconds}s)"
                )
                await self._end_session(session)

    async def _end_session(self, session: DeviceSession) -> None:
        """
        End a session and trigger archive creation.

        Args:
            session: The session to end
        """
        state_key = session.state_key

        # Remove from active sessions
        if state_key in self.sessions:
            del self.sessions[state_key]

        # Check if any segments were generated
        if session.first_segment_number < 0 or session.last_segment_number < 0:
            logger.info(
                f"Session has no segments to archive: {state_key} (no segments were generated)"
            )
            return

        # Check minimum duration
        if session.duration_seconds < self.config.min_session_duration_seconds:
            logger.info(
                f"Session too short to archive: {state_key} "
                f"({session.duration_seconds}s < {self.config.min_session_duration_seconds}s)"
            )
            return

        logger.info(
            f"Session ended: {state_key} session={session.session_id} "
            f"duration={session.duration_seconds}s segments={session.segment_count}"
        )

        # Trigger archive creation callback
        if self.on_session_ended:
            try:
                await self.on_session_ended(session)
            except Exception as e:
                logger.error(f"Error in session ended callback: {e}", exc_info=True)

    def get_active_sessions(self) -> list[DeviceSession]:
        """Get list of all active sessions."""
        return list(self.sessions.values())

    def get_session(self, client_id: str, device_id: str) -> DeviceSession | None:
        """Get session for a specific device."""
        state_key = f"{client_id}:{device_id}"
        return self.sessions.get(state_key)
