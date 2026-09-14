"""
Cleanup Service

Removes old HLS segments beyond the retention window (24 hours default).
Supports both filesystem and GCS storage backends.
"""

import asyncio
import contextlib
import time
from datetime import datetime, timedelta, timezone

from ..config.settings import settings
from ..utils.logger import get_logger
from ..utils.metrics import cleanup_duration_histogram, segments_deleted_total
from .redis_playlist_store import RedisPlaylistStore
from .storage_backend import StorageBackend, create_storage_backend

logger = get_logger(__name__)

# Source frame extensions written by the encoder, matched case-sensitively as
# the previous glob patterns were.
FRAME_SUFFIXES = (".jpg", ".jpeg", ".png")


class CleanupService:
    """
    Background service for cleaning up old HLS segments.

    Runs periodically to remove segments older than retention_hours.
    Supports both filesystem and GCS storage backends.
    """

    def __init__(self, storage: StorageBackend | None = None):
        """
        Initialize the cleanup service.

        Args:
            storage: Optional storage backend. If not provided, creates one from settings.
        """
        self.storage_config = settings.storage
        self.retention_hours = settings.processing.retention_hours
        self.running = False

        # Initialize storage backend
        if storage is not None:
            self.storage = storage
        else:
            self.storage = create_storage_backend(
                storage_type=self.storage_config.type,
                base_path=self.storage_config.base_path,
                gcs_bucket=self.storage_config.gcs_bucket,
                gcs_project_id=self.storage_config.gcs_project_id,
            )

        self.cleanup_interval_seconds = settings.processing.cleanup_interval_seconds

        # Set by stop() to cut the inter-cycle wait short, so shutdown does not
        # have to outlast a whole interval.
        self._stop_event = asyncio.Event()

        # Redis playlist store for removing segment metadata during cleanup
        self.playlist_store: RedisPlaylistStore | None = None

        if settings.redis.enabled and settings.redis.playlist_enabled:
            self.playlist_store = RedisPlaylistStore()
            logger.info("Redis playlist store enabled for cleanup synchronization")

        logger.info(f"Cleanup Service using {self.storage.get_storage_type()} storage backend")

    async def run(self) -> None:
        """
        Start the cleanup service.

        Runs periodically to clean up old segments.
        """
        logger.info("=" * 80)
        logger.info("Cleanup Service Started")
        logger.info(f"Retention: {self.retention_hours} hours")
        logger.info(f"Interval: {self.cleanup_interval_seconds} seconds")
        logger.info(f"Storage: {self.storage.get_storage_type()}")
        logger.info(f"Redis playlist store: {'enabled' if self.playlist_store else 'disabled'}")
        logger.info("=" * 80)

        if self._stop_event.is_set():
            logger.info("Cleanup service stopped before it started")
            return

        # Connect to Redis if playlist store is configured
        if self.playlist_store:
            await self.playlist_store.connect()

        # connect() awaits, so stop() can land inside it. Re-check: entering the
        # loop with the event already set would spin, since the wait below
        # returns instantly.
        if self._stop_event.is_set():
            logger.info("Cleanup service stopped before its first cycle")
            return

        self.running = True

        while not self._stop_event.is_set():
            try:
                await self._run_cleanup()
            except Exception as e:
                logger.error(f"Cleanup error: {e}", exc_info=True)

            # Wait for the next cycle, or return as soon as stop() fires.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.cleanup_interval_seconds
                )

        self.running = False

    async def stop(self) -> None:
        """Stop the cleanup service."""
        logger.info("Stopping cleanup service...")
        self.running = False
        self._stop_event.set()

        # Close Redis playlist store
        if self.playlist_store:
            try:
                await self.playlist_store.close()
            except Exception as e:
                logger.error(f"Error closing Redis playlist store: {e}")

    async def _run_cleanup(self) -> None:
        """
        Run a single cleanup cycle.

        Directory structure:
        {base_path}/client_ids/{client_id}/device_id/{device_id}/hls/segments/
        {base_path}/client_ids/{client_id}/device_id/{device_id}/frames/
        """
        start_time = time.time()

        # Calculate cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)  # noqa: UP017
        cutoff_timestamp = cutoff_time.timestamp()

        total_deleted = 0
        total_bytes_freed = 0

        # Materialize the device list once: on GCS every scan is a billable
        # listing, and this cycle needs the same list twice.
        devices = list(self.storage.list_all_devices())

        for client_id, device_id in devices:
            if self._stop_event.is_set():
                logger.info("Cleanup cycle interrupted by shutdown")
                break

            state_key = f"{client_id}:{device_id}"

            deleted_count, bytes_freed = await self._delete_old_segments(
                client_id, device_id, cutoff_timestamp
            )

            if deleted_count > 0:
                segments_deleted_total.labels(device_id=state_key).inc(deleted_count)
                logger.info(
                    f"Cleaned up {state_key}: "
                    f"{deleted_count} segments, {bytes_freed / 1024 / 1024:.2f} MB freed"
                )

            # Remove old segment metadata from Redis playlist store
            if self.playlist_store:
                try:
                    redis_removed = await self.playlist_store.remove_segments_before(
                        client_id, device_id, cutoff_timestamp
                    )
                    if redis_removed > 0:
                        logger.debug(
                            f"Removed {redis_removed} segment entries from Redis for {state_key}"
                        )
                except Exception as e:
                    logger.error(f"Error cleaning Redis playlist store for {state_key}: {e}")

            total_deleted += deleted_count
            total_bytes_freed += bytes_freed

        # Also clean up old source frames
        await self._cleanup_frames(cutoff_timestamp, devices)

        # Clean up stale temporary files (GCS backend downloads/intermediates)
        temp_removed = self.storage.cleanup_temp_files(max_age_seconds=600)
        if temp_removed > 0:
            logger.info(f"Cleaned up {temp_removed} stale temp files")

        duration = time.time() - start_time
        cleanup_duration_histogram.observe(duration)

        if total_deleted > 0:
            logger.info(
                f"Cleanup complete: {total_deleted} segments deleted, "
                f"{total_bytes_freed / 1024 / 1024:.2f} MB freed in {duration:.2f}s"
            )

    async def _delete_old_segments(
        self, client_id: str, device_id: str, cutoff_timestamp: float
    ) -> tuple[int, int]:
        """
        Delete one device's HLS segments older than the cutoff.

        Returns:
            (segments deleted, bytes freed)
        """
        deleted_count = 0
        bytes_freed = 0

        for file_info in self.storage.list_files(
            client_id, device_id, "hls/segments", pattern="seg_*.ts"
        ):
            if self._stop_event.is_set():
                break
            try:
                if file_info.mtime < cutoff_timestamp:
                    bytes_freed += file_info.size
                    if self.storage.delete_file(
                        client_id, device_id, f"hls/segments/{file_info.name}"
                    ):
                        deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting segment {file_info.name}: {e}")

            # Each delete is a blocking round trip and a backlogged device can
            # hold thousands. Yield so the frame consumer keeps running.
            await asyncio.sleep(0)

        return deleted_count, bytes_freed

    async def _cleanup_frames(
        self, cutoff_timestamp: float, devices: list[tuple[str, str]]
    ) -> None:
        """
        Clean up old source frames.

        Frames are deleted after they've been encoded into segments
        and are older than retention period.

        Args:
            cutoff_timestamp: Delete frames modified before this Unix timestamp
            devices: Client/device pairs from the caller's single storage scan

        Directory structure:
        {base_path}/client_ids/{client_id}/device_id/{device_id}/frames/
        """
        deleted_count = 0

        for client_id, device_id in devices:
            if self._stop_event.is_set():
                break
            deleted_count += await self._delete_old_frames(client_id, device_id, cutoff_timestamp)

        if deleted_count > 0:
            logger.debug(f"Cleaned up {deleted_count} old source frames")

    async def _delete_old_frames(
        self, client_id: str, device_id: str, cutoff_timestamp: float
    ) -> int:
        """
        Delete one device's source frames older than the cutoff.

        Returns:
            Number of frames deleted
        """
        deleted_count = 0

        # One listing, filtered here. list_files enumerates the whole directory
        # server-side whatever the pattern, so a pattern per extension would
        # bill the same listing three times.
        for file_info in self.storage.list_files(client_id, device_id, "frames"):
            if self._stop_event.is_set():
                break
            if not file_info.name.endswith(FRAME_SUFFIXES):
                continue
            try:
                if file_info.mtime < cutoff_timestamp and self.storage.delete_file(
                    client_id, device_id, f"frames/{file_info.name}"
                ):
                    deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting frame {file_info.name}: {e}")

            await asyncio.sleep(0)

        return deleted_count
