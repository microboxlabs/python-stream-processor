"""
Redis-based Session Store

Stores device session state in Redis for distributed offline detection.
Used by:
- Consumer: Updates session activity on frame receipt
- Offline Checker: Detects offline devices and triggers archiving
"""

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import redis.asyncio as redis

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

# Redis key prefixes
SESSION_KEY_PREFIX = "stream:session:"
SESSION_INDEX_KEY = "stream:sessions"


@dataclass
class SessionData:
    """Session data stored in Redis."""

    client_id: str
    device_id: str
    session_id: str
    started_at: str  # ISO format
    last_frame_at: str  # ISO format
    first_segment_number: int
    last_segment_number: int
    frame_count: int

    @property
    def state_key(self) -> str:
        """Get the state key for this device."""
        return f"{self.client_id}:{self.device_id}"

    @property
    def started_at_dt(self) -> datetime:
        """Get started_at as datetime."""
        return datetime.fromisoformat(self.started_at)

    @property
    def last_frame_at_dt(self) -> datetime:
        """Get last_frame_at as datetime."""
        return datetime.fromisoformat(self.last_frame_at)

    @property
    def duration_seconds(self) -> int:
        """Get the session duration in seconds."""
        return int((self.last_frame_at_dt - self.started_at_dt).total_seconds())

    @property
    def segment_count(self) -> int:
        """Get the number of segments in this session."""
        if self.first_segment_number < 0 or self.last_segment_number < 0:
            return 0
        return self.last_segment_number - self.first_segment_number + 1

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "SessionData":
        """Deserialize from JSON."""
        return cls(**json.loads(data))


class RedisSessionStore:
    """
    Redis-backed session store for distributed offline detection.

    Session data is stored as JSON strings with the key pattern:
    stream:session:{client_id}:{device_id}

    A set of active session keys is maintained at:
    stream:sessions
    """

    def __init__(self, redis_url: str | None = None):
        """
        Initialize the Redis session store.

        Args:
            redis_url: Redis connection URL. If not provided, uses settings.
        """
        self.redis_url = redis_url or settings.redis.url
        self._client: redis.Redis | None = None

    async def connect(self) -> redis.Redis:
        """Connect to Redis and return the client."""
        if self._client is None:
            self._client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            await self._client.ping()  # type: ignore[misc]
            logger.info(f"Connected to Redis at {self.redis_url}")
        return self._client

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Redis connection closed")

    def _session_key(self, client_id: str, device_id: str) -> str:
        """Get Redis key for a session."""
        return f"{SESSION_KEY_PREFIX}{client_id}:{device_id}"

    async def update_activity(
        self,
        client_id: str,
        device_id: str,
    ) -> SessionData:
        """
        Update session activity timestamp.

        Creates a new session if one doesn't exist.
        Called by consumers on every frame receipt.

        Args:
            client_id: Client identifier
            device_id: Device identifier

        Returns:
            The updated SessionData
        """
        client = await self.connect()

        key = self._session_key(client_id, device_id)
        now = datetime.now(UTC).isoformat()

        # Try to get existing session
        existing = await client.get(key)

        if existing:
            session = SessionData.from_json(existing)
            session.last_frame_at = now
            session.frame_count += 1
        else:
            # Create new session
            session = SessionData(
                client_id=client_id,
                device_id=device_id,
                session_id=str(uuid.uuid4()),
                started_at=now,
                last_frame_at=now,
                first_segment_number=-1,
                last_segment_number=-1,
                frame_count=1,
            )
            logger.info(f"New session started: {session.state_key} session={session.session_id}")
            # Add to active sessions index
            await client.sadd(SESSION_INDEX_KEY, session.state_key)  # type: ignore[misc]

        # Save session (with 24h expiry as safety net)
        await client.set(key, session.to_json(), ex=86400)

        return session

    async def update_segment(
        self,
        client_id: str,
        device_id: str,
        segment_number: int,
    ) -> SessionData | None:
        """
        Update session with segment generation info.

        Called by consumers when a segment is generated.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            segment_number: Current segment number

        Returns:
            The updated SessionData, or None if session doesn't exist
        """
        client = await self.connect()

        key = self._session_key(client_id, device_id)

        existing = await client.get(key)
        if not existing:
            logger.warning(f"No active session for segment update: {client_id}:{device_id}")
            return None

        session = SessionData.from_json(existing)

        # Set first segment number if not yet set
        if session.first_segment_number < 0:
            session.first_segment_number = segment_number
            logger.info(
                f"Session first segment recorded: {session.state_key} segment={segment_number}"
            )

        # Update last segment number
        if segment_number > session.last_segment_number:
            session.last_segment_number = segment_number

        # Save session
        await client.set(key, session.to_json(), ex=86400)

        return session

    async def get_session(self, client_id: str, device_id: str) -> SessionData | None:
        """Get session data for a device."""
        client = await self.connect()

        key = self._session_key(client_id, device_id)
        data = await client.get(key)

        if data:
            return SessionData.from_json(data)
        return None

    async def get_all_sessions(self) -> list[SessionData]:
        """Get all active sessions."""
        client = await self.connect()

        sessions = []

        # Get all session keys from index
        state_keys = await client.smembers(SESSION_INDEX_KEY)  # type: ignore[misc]

        for state_key in state_keys:
            parts = state_key.split(":", 1)
            if len(parts) == 2:
                client_id, device_id = parts
                session = await self.get_session(client_id, device_id)
                if session:
                    sessions.append(session)
                else:
                    # Clean up stale index entry
                    await client.srem(SESSION_INDEX_KEY, state_key)  # type: ignore[misc]

        return sessions

    async def delete_session(self, client_id: str, device_id: str) -> bool:
        """
        Delete a session.

        Called when a session ends (device goes offline).

        Returns:
            True if session existed and was deleted
        """
        client = await self.connect()

        key = self._session_key(client_id, device_id)
        state_key = f"{client_id}:{device_id}"

        # Remove from index
        await client.srem(SESSION_INDEX_KEY, state_key)  # type: ignore[misc]

        # Delete session data
        deleted: int = await client.delete(key)

        return deleted > 0

    async def restart_session(
        self,
        client_id: str,
        device_id: str,
    ) -> SessionData:
        """
        Restart a session with a new session_id for max duration breaking.

        The device is still active, so we create a new session that will
        continue tracking from whatever segment the consumer generates next.

        Args:
            client_id: Client identifier
            device_id: Device identifier

        Returns:
            The new SessionData
        """
        client = await self.connect()
        key = self._session_key(client_id, device_id)
        now = datetime.now(UTC).isoformat()

        # Create new session with fresh session_id
        session = SessionData(
            client_id=client_id,
            device_id=device_id,
            session_id=str(uuid.uuid4()),
            started_at=now,
            last_frame_at=now,
            first_segment_number=-1,  # Will be set on first segment of new session
            last_segment_number=-1,
            frame_count=0,
        )

        logger.info(
            f"Session restarted (max duration): {session.state_key} "
            f"new_session={session.session_id}"
        )

        # Save new session (index entry already exists)
        await client.set(key, session.to_json(), ex=86400)
        return session
