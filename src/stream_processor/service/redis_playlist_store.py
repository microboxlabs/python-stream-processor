"""
Redis-based Playlist Store

Stores HLS segment metadata in Redis for dynamic playlist generation.
Used by:
- Consumer: Adds segment entry after successful generation
- Cleanup Service: Removes segment entry when deleting from storage
- Playlist Service (quarkus): Queries segments for on-the-fly playlist generation
"""

import time
from urllib.parse import urlparse

import redis.asyncio as redis

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

# Redis key prefix for segment metadata
SEGMENTS_KEY_PREFIX = "hls:segments:"


class RedisPlaylistStore:
    """
    Redis-backed playlist store for dynamic HLS playlist generation.

    Segment metadata is stored as a sorted set (ZSET) with:
    - Key: hls:segments:{client_id}:{device_id}
    - Score: Unix timestamp (when segment was created)
    - Member: Segment number (as string)

    This allows efficient time-range queries for playlist generation.
    """

    def __init__(self, redis_url: str | None = None):
        """
        Initialize the Redis playlist store.

        Args:
            redis_url: Redis connection URL. If not provided, uses settings.
        """
        self.redis_url = redis_url or settings.redis.url
        self._client: redis.Redis | None = None

    def _redact_url(self, url: str) -> str:
        """Redact credentials from a URL for safe logging."""
        parsed = urlparse(url)
        if parsed.password:
            # Replace password with *** in netloc
            redacted_netloc = parsed.netloc.replace(f":{parsed.password}@", ":***@")
            return url.replace(parsed.netloc, redacted_netloc)
        return url

    async def connect(self) -> redis.Redis:
        """Connect to Redis and return the client."""
        if self._client is None:
            self._client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            await self._client.ping()  # type: ignore[misc]
            logger.info(f"Playlist store connected to Redis at {self._redact_url(self.redis_url)}")
        return self._client

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Playlist store Redis connection closed")

    def _segments_key(self, client_id: str, device_id: str) -> str:
        """Get Redis key for segment metadata."""
        return f"{SEGMENTS_KEY_PREFIX}{client_id}:{device_id}"

    async def add_segment(
        self,
        client_id: str,
        device_id: str,
        segment_number: int,
        timestamp: float | None = None,
    ) -> bool:
        """
        Add a segment to the playlist metadata.

        Called after successful segment generation.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            segment_number: Segment sequence number
            timestamp: Unix timestamp when segment was created (defaults to now)

        Returns:
            True if segment was added (new), False if it already existed
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        if timestamp is None:
            timestamp = time.time()

        # ZADD returns 1 if new member added, 0 if score updated
        added: int = await client.zadd(key, {str(segment_number): timestamp})  # type: ignore[misc]

        if added:
            logger.debug(
                f"Added segment to playlist store: {client_id}:{device_id} "
                f"segment={segment_number} timestamp={timestamp}"
            )

        return added > 0

    async def remove_segment(
        self,
        client_id: str,
        device_id: str,
        segment_number: int,
    ) -> bool:
        """
        Remove a segment from the playlist metadata.

        Called when a segment is deleted from storage.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            segment_number: Segment sequence number

        Returns:
            True if segment was removed, False if it didn't exist
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        # ZREM returns number of members removed
        removed: int = await client.zrem(key, str(segment_number))  # type: ignore[misc]

        if removed:
            logger.debug(
                f"Removed segment from playlist store: {client_id}:{device_id} "
                f"segment={segment_number}"
            )

        return removed > 0

    async def remove_segments_before(
        self,
        client_id: str,
        device_id: str,
        cutoff_timestamp: float,
    ) -> int:
        """
        Remove all segments older than the cutoff timestamp.

        Called during cleanup to remove stale segment entries.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            cutoff_timestamp: Unix timestamp; segments older than this are removed

        Returns:
            Number of segments removed
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        # ZREMRANGEBYSCORE removes members with scores in the given range
        # Using -inf to cutoff_timestamp to remove all old segments
        removed: int = await client.zremrangebyscore(  # type: ignore[misc]
            key, "-inf", cutoff_timestamp
        )

        if removed:
            logger.debug(
                f"Removed {removed} old segments from playlist store: "
                f"{client_id}:{device_id} cutoff={cutoff_timestamp}"
            )

        return removed

    async def get_segments(
        self,
        client_id: str,
        device_id: str,
        from_timestamp: float | None = None,
        to_timestamp: float | None = None,
    ) -> list[tuple[int, float]]:
        """
        Get segments in a time range.

        Used by playlist service to generate playlists on-the-fly.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            from_timestamp: Start of time range (default: 24 hours ago)
            to_timestamp: End of time range (default: now)

        Returns:
            List of (segment_number, timestamp) tuples, ordered by timestamp
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        if to_timestamp is None:
            to_timestamp = time.time()

        if from_timestamp is None:
            # Default to 24 hours of retention
            from_timestamp = to_timestamp - (settings.processing.retention_hours * 3600)

        # ZRANGEBYSCORE returns members with scores in range, with scores
        results = await client.zrangebyscore(  # type: ignore[misc]
            key, from_timestamp, to_timestamp, withscores=True
        )

        # Convert to list of (segment_number, timestamp) tuples
        segments = [(int(member), score) for member, score in results]

        logger.debug(
            f"Retrieved {len(segments)} segments from playlist store: "
            f"{client_id}:{device_id} range=[{from_timestamp}, {to_timestamp}]"
        )

        return segments

    async def get_segment_count(self, client_id: str, device_id: str) -> int:
        """
        Get the number of segments in the playlist.

        Args:
            client_id: Client identifier
            device_id: Device identifier

        Returns:
            Number of segments
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        count: int = await client.zcard(key)  # type: ignore[misc]
        return count

    async def delete_playlist(self, client_id: str, device_id: str) -> bool:
        """
        Delete all segment metadata for a device.

        Called when a device is completely removed.

        Args:
            client_id: Client identifier
            device_id: Device identifier

        Returns:
            True if the key existed and was deleted
        """
        client = await self.connect()
        key = self._segments_key(client_id, device_id)

        deleted: int = await client.delete(key)

        if deleted:
            logger.info(f"Deleted playlist from store: {client_id}:{device_id}")

        return deleted > 0
