"""
Offline Checker Service

Standalone service that monitors Redis for offline devices and triggers
archive creation. Designed to run as a separate process/container.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from ..config.settings import settings
from ..utils.logger import get_logger
from .archive_service import ArchiveService
from .redis_session_store import RedisSessionStore, SessionData
from .session_tracker import DeviceSession

logger = get_logger(__name__)


class OfflineChecker:
    """
    Standalone offline detection service.

    Periodically checks Redis for sessions that have gone offline
    (no frame updates for offline_threshold_seconds) and triggers
    archive creation.

    Can be run as:
    - A separate container/deployment
    - A Kubernetes CronJob (one-shot mode)
    """

    def __init__(
        self,
        session_store: RedisSessionStore | None = None,
        archive_service: ArchiveService | None = None,
        check_interval: int = 10,
    ):
        """
        Initialize the offline checker.

        Args:
            session_store: Redis session store. If not provided, creates one.
            archive_service: Archive service. If not provided, creates one.
            check_interval: Seconds between checks (for continuous mode)
        """
        self.config = settings.archive
        self.session_store = session_store or RedisSessionStore()
        self.archive_service = archive_service or ArchiveService()
        self.check_interval = check_interval
        self.running = False

    async def check_once(self) -> int:
        """
        Check for offline devices once.

        Returns:
            Number of offline sessions detected and processed
        """
        await self.session_store.connect()

        now = datetime.now(UTC)
        threshold = timedelta(seconds=self.config.offline_threshold_seconds)
        offline_count = 0

        sessions = await self.session_store.get_all_sessions()

        logger.info(
            f"[OfflineChecker] Checking {len(sessions)} active session(s), "
            f"threshold={self.config.offline_threshold_seconds}s"
        )

        for session in sessions:
            idle_time = now - session.last_frame_at_dt
            idle_seconds = idle_time.total_seconds()

            logger.debug(
                f"[OfflineChecker] {session.state_key}: idle={idle_seconds:.1f}s, "
                f"duration={session.duration_seconds}s, frames={session.frame_count}, "
                f"segments={session.first_segment_number}-{session.last_segment_number}"
            )

            if idle_time > threshold:
                logger.info(
                    f"Device offline detected: {session.state_key} "
                    f"(last frame {idle_seconds:.0f}s ago, "
                    f"session duration={session.duration_seconds}s)"
                )
                await self._end_session(session)
                offline_count += 1

        return offline_count

    async def _end_session(self, session: SessionData) -> None:
        """
        End a session and trigger archive creation.

        Args:
            session: The session data from Redis
        """
        state_key = session.state_key

        # Delete from Redis first to prevent duplicate processing
        await self.session_store.delete_session(session.client_id, session.device_id)

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

        # Convert to DeviceSession for archive service
        device_session = DeviceSession(
            client_id=session.client_id,
            device_id=session.device_id,
            session_id=session.session_id,
            started_at=session.started_at_dt,
            last_frame_at=session.last_frame_at_dt,
            first_segment_number=session.first_segment_number,
            last_segment_number=session.last_segment_number,
            frame_count=session.frame_count,
        )

        # Create archive
        try:
            logger.info(
                f"Creating archive for ended session: {state_key} session={session.session_id}"
            )
            await self.archive_service.create_archive(device_session)
        except Exception as e:
            logger.error(f"Error creating archive for {state_key}: {e}", exc_info=True)

    async def run_continuous(self) -> None:
        """
        Run the offline checker continuously.

        Checks for offline devices every check_interval seconds.
        """
        if not self.config.enabled:
            logger.info("Archive/deferred transmissions disabled")
            return

        logger.info("=" * 80)
        logger.info("Offline Checker Service Started (Continuous Mode)")
        logger.info(f"Offline threshold: {self.config.offline_threshold_seconds}s")
        logger.info(f"Min session duration: {self.config.min_session_duration_seconds}s")
        logger.info(f"Retention: {self.config.retention_days} days")
        logger.info(f"Check interval: {self.check_interval}s")
        logger.info("=" * 80)

        self.running = True

        try:
            while self.running:
                try:
                    await self.check_once()
                except Exception as e:
                    logger.error(f"Error during offline check: {e}", exc_info=True)

                await asyncio.sleep(self.check_interval)

        finally:
            await self.close()

    async def run_once(self) -> int:
        """
        Run the offline checker once and exit.

        Useful for Kubernetes CronJob deployments.

        Returns:
            Number of offline sessions processed
        """
        if not self.config.enabled:
            logger.info("Archive/deferred transmissions disabled")
            return 0

        logger.info("=" * 80)
        logger.info("Offline Checker Service (One-Shot Mode)")
        logger.info(f"Offline threshold: {self.config.offline_threshold_seconds}s")
        logger.info(f"Min session duration: {self.config.min_session_duration_seconds}s")
        logger.info("=" * 80)

        try:
            count = await self.check_once()
            logger.info(f"Offline check complete: {count} session(s) processed")
            return count
        finally:
            await self.close()

    async def stop(self) -> None:
        """Stop the continuous checker."""
        logger.info("Stopping offline checker...")
        self.running = False

    async def close(self) -> None:
        """Close resources."""
        await self.session_store.close()
        await self.archive_service.close()


async def run_offline_checker(continuous: bool = True, interval: int = 10) -> None:
    """
    Main entry point for the offline checker service.

    Args:
        continuous: If True, run continuously. If False, run once and exit.
        interval: Check interval in seconds (for continuous mode)
    """
    checker = OfflineChecker(check_interval=interval)

    if continuous:
        await checker.run_continuous()
    else:
        await checker.run_once()
