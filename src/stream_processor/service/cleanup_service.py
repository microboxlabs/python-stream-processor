"""
Cleanup Service

Removes old HLS segments beyond the retention window (24 hours default).
Supports both filesystem and GCS storage backends.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

from ..config.settings import settings
from ..utils.logger import get_logger
from ..utils.metrics import cleanup_duration_histogram, segments_deleted_total
from .storage_backend import StorageBackend, create_storage_backend

logger = get_logger(__name__)


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

        # Run cleanup every 5 minutes
        self.cleanup_interval_seconds = 300

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
        logger.info("=" * 80)

        self.running = True

        while self.running:
            try:
                await self._run_cleanup()
            except Exception as e:
                logger.error(f"Cleanup error: {e}", exc_info=True)

            # Wait for next cleanup cycle
            await asyncio.sleep(self.cleanup_interval_seconds)

    async def stop(self) -> None:
        """Stop the cleanup service."""
        logger.info("Stopping cleanup service...")
        self.running = False

    async def _run_cleanup(self) -> None:
        """
        Run a single cleanup cycle.

        Directory structure:
        {base_path}/client_ids/{client_id}/device_id/{device_id}/hls/segments/
        {base_path}/client_ids/{client_id}/device_id/{device_id}/frames/
        """
        start_time = time.time()

        # Calculate cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        cutoff_timestamp = cutoff_time.timestamp()

        total_deleted = 0
        total_bytes_freed = 0

        # Iterate through all devices using the storage backend
        for client_id, device_id in self.storage.list_all_devices():
            state_key = f"{client_id}:{device_id}"

            # Clean up old segments
            deleted_count = 0
            bytes_freed = 0

            for file_info in self.storage.list_files(
                client_id, device_id, "hls/segments", pattern="seg_*.ts"
            ):
                try:
                    # Check file modification time
                    if file_info.mtime < cutoff_timestamp:
                        bytes_freed += file_info.size
                        if self.storage.delete_file(
                            client_id, device_id, f"hls/segments/{file_info.name}"
                        ):
                            deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting segment {file_info.name}: {e}")

            if deleted_count > 0:
                segments_deleted_total.labels(device_id=state_key).inc(deleted_count)
                logger.info(
                    f"Cleaned up {state_key}: "
                    f"{deleted_count} segments, {bytes_freed / 1024 / 1024:.2f} MB freed"
                )

            total_deleted += deleted_count
            total_bytes_freed += bytes_freed

        # Also clean up old source frames
        await self._cleanup_frames(cutoff_timestamp)

        duration = time.time() - start_time
        cleanup_duration_histogram.observe(duration)

        if total_deleted > 0:
            logger.info(
                f"Cleanup complete: {total_deleted} segments deleted, "
                f"{total_bytes_freed / 1024 / 1024:.2f} MB freed in {duration:.2f}s"
            )

    async def _cleanup_frames(self, cutoff_timestamp: float) -> None:
        """
        Clean up old source frames.

        Frames are deleted after they've been encoded into segments
        and are older than retention period.

        Directory structure:
        {base_path}/client_ids/{client_id}/device_id/{device_id}/frames/
        """
        deleted_count = 0

        # Iterate through all devices
        for client_id, device_id in self.storage.list_all_devices():
            # Clean up old frames (jpg and png)
            for pattern in ["*.jpg", "*.jpeg", "*.png"]:
                for file_info in self.storage.list_files(
                    client_id, device_id, "frames", pattern=pattern
                ):
                    try:
                        if file_info.mtime < cutoff_timestamp:
                            if self.storage.delete_file(
                                client_id, device_id, f"frames/{file_info.name}"
                            ):
                                deleted_count += 1
                    except Exception as e:
                        logger.error(f"Error deleting frame {file_info.name}: {e}")

        if deleted_count > 0:
            logger.debug(f"Cleaned up {deleted_count} old source frames")
