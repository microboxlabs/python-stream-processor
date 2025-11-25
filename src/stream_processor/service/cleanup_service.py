"""
Cleanup Service

Removes old HLS segments beyond the retention window (24 hours default).
"""

import asyncio
import os
import time
from pathlib import Path
from datetime import datetime, timedelta

from ..config.settings import settings
from ..utils.logger import get_logger
from ..utils.metrics import segments_deleted_total, cleanup_duration_histogram

logger = get_logger(__name__)


class CleanupService:
    """
    Background service for cleaning up old HLS segments.
    
    Runs periodically to remove segments older than retention_hours.
    """

    def __init__(self):
        """Initialize the cleanup service."""
        self.storage = settings.storage
        self.retention_hours = settings.processing.retention_hours
        self.running = False
        
        # Run cleanup every 5 minutes
        self.cleanup_interval_seconds = 300

    async def run(self) -> None:
        """
        Start the cleanup service.
        
        Runs periodically to clean up old segments.
        """
        logger.info("=" * 80)
        logger.info("Cleanup Service Started")
        logger.info(f"Retention: {self.retention_hours} hours")
        logger.info(f"Interval: {self.cleanup_interval_seconds} seconds")
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
        """Run a single cleanup cycle."""
        start_time = time.time()
        
        hls_path = Path(self.storage.hls_path)
        if not hls_path.exists():
            return
        
        # Calculate cutoff time
        cutoff_time = datetime.utcnow() - timedelta(hours=self.retention_hours)
        cutoff_timestamp = cutoff_time.timestamp()
        
        total_deleted = 0
        total_bytes_freed = 0
        
        # Iterate through device directories
        for device_dir in hls_path.iterdir():
            if not device_dir.is_dir():
                continue
            
            device_id = device_dir.name
            segments_dir = device_dir / "segments"
            
            if not segments_dir.exists():
                continue
            
            # Find and delete old segments
            deleted_count = 0
            bytes_freed = 0
            
            for segment_file in segments_dir.glob("seg_*.ts"):
                try:
                    # Check file modification time
                    mtime = segment_file.stat().st_mtime
                    
                    if mtime < cutoff_timestamp:
                        file_size = segment_file.stat().st_size
                        segment_file.unlink()
                        deleted_count += 1
                        bytes_freed += file_size
                        
                except Exception as e:
                    logger.error(f"Error deleting {segment_file}: {e}")
            
            if deleted_count > 0:
                segments_deleted_total.labels(device_id=device_id).inc(deleted_count)
                logger.info(
                    f"Cleaned up {device_id}: "
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
        """
        frames_path = Path(self.storage.frames_path)
        if not frames_path.exists():
            return
        
        deleted_count = 0
        
        for device_dir in frames_path.iterdir():
            if not device_dir.is_dir():
                continue
            
            for frame_file in device_dir.glob("*.jpg"):
                try:
                    mtime = frame_file.stat().st_mtime
                    
                    if mtime < cutoff_timestamp:
                        frame_file.unlink()
                        deleted_count += 1
                        
                except Exception as e:
                    logger.error(f"Error deleting frame {frame_file}: {e}")
            
            # Clean up PNG frames too
            for frame_file in device_dir.glob("*.png"):
                try:
                    mtime = frame_file.stat().st_mtime
                    
                    if mtime < cutoff_timestamp:
                        frame_file.unlink()
                        deleted_count += 1
                        
                except Exception as e:
                    logger.error(f"Error deleting frame {frame_file}: {e}")
        
        if deleted_count > 0:
            logger.debug(f"Cleaned up {deleted_count} old source frames")

