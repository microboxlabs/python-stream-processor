"""
Device Reset Service

Resets all data for a specific device across Redis, storage, and database.
Used for troubleshooting and testing purposes.
"""

from dataclasses import dataclass, field

from ..config.settings import settings
from ..utils.logger import get_logger
from .redis_playlist_store import RedisPlaylistStore
from .redis_session_store import RedisSessionStore
from .storage_backend import StorageBackend, create_storage_backend

logger = get_logger(__name__)


@dataclass
class ResetResult:
    """Result of a device reset operation."""

    redis_segments_deleted: int = 0
    redis_session_deleted: bool = False
    storage_frames_deleted: int = 0
    storage_segments_deleted: int = 0
    storage_playlist_deleted: bool = False
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            "redis_segments_deleted": self.redis_segments_deleted,
            "redis_session_deleted": self.redis_session_deleted,
            "storage_frames_deleted": self.storage_frames_deleted,
            "storage_segments_deleted": self.storage_segments_deleted,
            "storage_playlist_deleted": self.storage_playlist_deleted,
            "errors": self.errors if self.errors else "none",
        }


class DeviceResetService:
    """
    Service for resetting all data associated with a device.

    Cleans up:
    - Redis: Playlist segments (hls:segments:*), session data (stream:session:*)
    - Storage: Frames, HLS segments, playlist.m3u8
    """

    def __init__(self):
        """Initialize the reset service."""
        self.storage: StorageBackend = create_storage_backend(
            storage_type=settings.storage.type,
            base_path=settings.storage.base_path,
            gcs_bucket=settings.storage.gcs_bucket,
            gcs_project_id=settings.storage.gcs_project_id,
        )

        # Redis stores (initialized lazily)
        self.playlist_store: RedisPlaylistStore | None = None
        self.session_store: RedisSessionStore | None = None

        if settings.redis.enabled:
            self.playlist_store = RedisPlaylistStore()
            self.session_store = RedisSessionStore()

    async def close(self) -> None:
        """Close all connections."""
        if self.playlist_store:
            try:
                await self.playlist_store.close()
            except Exception:
                pass

        if self.session_store:
            try:
                await self.session_store.close()
            except Exception:
                pass

    async def reset_device(
        self,
        client_id: str,
        device_id: str,
        dry_run: bool = False,
        skip_redis: bool = False,
        skip_storage: bool = False,
    ) -> dict:
        """
        Reset all data for a specific device.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            dry_run: If True, only report what would be deleted
            skip_redis: Skip Redis cleanup
            skip_storage: Skip storage cleanup

        Returns:
            Dictionary with reset results
        """
        result = ResetResult()

        logger.info(f"Starting device reset for {client_id}:{device_id}")

        # Reset Redis data
        if not skip_redis:
            await self._reset_redis(client_id, device_id, dry_run, result)

        # Reset storage data
        if not skip_storage:
            await self._reset_storage(client_id, device_id, dry_run, result)

        return result.to_dict()

    async def _reset_redis(
        self,
        client_id: str,
        device_id: str,
        dry_run: bool,
        result: ResetResult,
    ) -> None:
        """Reset Redis data for the device."""
        if not settings.redis.enabled:
            logger.info("Redis not enabled, skipping Redis reset")
            return

        # Reset playlist segments
        if self.playlist_store:
            try:
                await self.playlist_store.connect()

                # Get current segment count
                count = await self.playlist_store.get_segment_count(client_id, device_id)
                result.redis_segments_deleted = count

                if count > 0:
                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would delete {count} segments from Redis "
                            f"for {client_id}:{device_id}"
                        )
                    else:
                        deleted = await self.playlist_store.delete_playlist(client_id, device_id)
                        logger.info(
                            f"Deleted playlist from Redis for {client_id}:{device_id}: {deleted}"
                        )
                else:
                    logger.info(f"No segments in Redis for {client_id}:{device_id}")

            except Exception as e:
                error_msg = f"Error resetting Redis playlist: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)

        # Reset session data
        if self.session_store:
            try:
                await self.session_store.connect()

                # Check if session exists
                session = await self.session_store.get_session(client_id, device_id)
                if session:
                    result.redis_session_deleted = True

                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would delete session from Redis "
                            f"for {client_id}:{device_id}"
                        )
                    else:
                        await self.session_store.delete_session(client_id, device_id)
                        logger.info(f"Deleted session from Redis for {client_id}:{device_id}")
                else:
                    logger.info(f"No session in Redis for {client_id}:{device_id}")

            except Exception as e:
                error_msg = f"Error resetting Redis session: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)

    async def _reset_storage(
        self,
        client_id: str,
        device_id: str,
        dry_run: bool,
        result: ResetResult,
    ) -> None:
        """Reset storage data for the device."""
        # Delete frames
        try:
            frames_deleted = 0
            for pattern in ["*.jpg", "*.jpeg", "*.png"]:
                for file_info in self.storage.list_files(client_id, device_id, "frames", pattern):
                    if dry_run:
                        logger.debug(f"[DRY RUN] Would delete frame: {file_info.name}")
                    else:
                        self.storage.delete_file(client_id, device_id, f"frames/{file_info.name}")
                    frames_deleted += 1

            result.storage_frames_deleted = frames_deleted
            if frames_deleted > 0:
                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would delete {frames_deleted} frames "
                        f"for {client_id}:{device_id}"
                    )
                else:
                    logger.info(f"Deleted {frames_deleted} frames for {client_id}:{device_id}")
            else:
                logger.info(f"No frames to delete for {client_id}:{device_id}")

        except Exception as e:
            error_msg = f"Error deleting frames: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)

        # Delete HLS segments
        try:
            segments_deleted = 0
            for file_info in self.storage.list_files(
                client_id, device_id, "hls/segments", "seg_*.ts"
            ):
                if dry_run:
                    logger.debug(f"[DRY RUN] Would delete segment: {file_info.name}")
                else:
                    self.storage.delete_file(
                        client_id, device_id, f"hls/segments/{file_info.name}"
                    )
                segments_deleted += 1

            result.storage_segments_deleted = segments_deleted
            if segments_deleted > 0:
                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would delete {segments_deleted} HLS segments "
                        f"for {client_id}:{device_id}"
                    )
                else:
                    logger.info(
                        f"Deleted {segments_deleted} HLS segments for {client_id}:{device_id}"
                    )
            else:
                logger.info(f"No HLS segments to delete for {client_id}:{device_id}")

        except Exception as e:
            error_msg = f"Error deleting HLS segments: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)

        # Delete playlist.m3u8
        try:
            # Check if playlist exists by trying to read it
            playlist_data = self.storage.read_file(client_id, device_id, "hls/playlist.m3u8")
            if playlist_data:
                result.storage_playlist_deleted = True
                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would delete playlist.m3u8 for {client_id}:{device_id}"
                    )
                else:
                    self.storage.delete_file(client_id, device_id, "hls/playlist.m3u8")
                    logger.info(f"Deleted playlist.m3u8 for {client_id}:{device_id}")
            else:
                logger.info(f"No playlist.m3u8 to delete for {client_id}:{device_id}")

        except Exception as e:
            error_msg = f"Error deleting playlist: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)
