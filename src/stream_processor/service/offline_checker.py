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
        cleanup_interval_checks: int = 60,
    ):
        """
        Initialize the offline checker.

        Args:
            session_store: Redis session store. If not provided, creates one.
            archive_service: Archive service. If not provided, creates one.
            check_interval: Seconds between checks (for continuous mode)
            cleanup_interval_checks: Number of checks between archive cleanup runs
                                     (default 60 = ~10 minutes at 10s check interval)
        """
        self.config = settings.archive
        self.session_store = session_store or RedisSessionStore()
        self.archive_service = archive_service or ArchiveService()
        self.check_interval = check_interval
        self.cleanup_interval_checks = cleanup_interval_checks
        self.running = False

    async def check_once(self) -> int:
        """
        Check for offline devices and sessions exceeding max duration.

        Returns:
            Number of sessions processed (offline + max duration exceeded)
        """
        await self.session_store.connect()

        now = datetime.now(UTC)
        threshold = timedelta(seconds=self.config.offline_threshold_seconds)
        max_duration = self.config.max_session_duration_seconds
        processed_count = 0

        sessions = await self.session_store.get_all_sessions()

        logger.info(
            f"[OfflineChecker] Checking {len(sessions)} active session(s), "
            f"offline_threshold={self.config.offline_threshold_seconds}s, "
            f"max_duration={max_duration}s"
        )

        for session in sessions:
            idle_time = now - session.last_frame_at_dt
            idle_seconds = idle_time.total_seconds()

            logger.debug(
                f"[OfflineChecker] {session.state_key}: idle={idle_seconds:.1f}s, "
                f"duration={session.duration_seconds}s, frames={session.frame_count}, "
                f"segments={session.first_segment_number}-{session.last_segment_number}"
            )

            # Check offline first (device stopped sending)
            if idle_time > threshold:
                logger.info(
                    f"Device offline detected: {session.state_key} "
                    f"(last frame {idle_seconds:.0f}s ago, "
                    f"session duration={session.duration_seconds}s)"
                )
                await self._end_session(session)
                processed_count += 1

            # Check max duration (device still active but session too long)
            elif max_duration > 0 and session.duration_seconds >= max_duration:
                logger.info(
                    f"Max duration exceeded: {session.state_key} "
                    f"(duration={session.duration_seconds}s >= {max_duration}s)"
                )
                await self._break_session_for_duration(session)
                processed_count += 1

        return processed_count

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
            captured_started_at=session.captured_started_dt,
            captured_ended_at=session.captured_ended_dt,
        )

        # Create archive
        try:
            logger.info(
                f"Creating archive for ended session: {state_key} session={session.session_id}"
            )
            await self.archive_service.create_archive(device_session)
        except Exception as e:
            logger.error(f"Error creating archive for {state_key}: {e}", exc_info=True)

    async def _break_session_for_duration(self, session: SessionData) -> None:
        """
        Break a session due to max duration exceeded and start a new one.

        The device is still actively sending frames, so we:
        1. Re-read the latest session data to capture any segments that arrived since snapshot
        2. Archive the current session
        3. Create a new session that continues seamlessly

        Args:
            session: The session data from Redis (may be stale)
        """
        state_key = session.state_key
        original_session_id = session.session_id

        # Re-read the latest session data to capture any segments that arrived
        # between the check_once snapshot and now
        latest_session = await self.session_store.get_session(session.client_id, session.device_id)

        if latest_session is None:
            logger.warning(
                f"Session disappeared before break (max duration): {state_key} "
                f"session={original_session_id}"
            )
            return

        # Verify session_id hasn't changed (another process might have restarted it)
        if latest_session.session_id != original_session_id:
            logger.info(
                f"Session already restarted by another process (max duration): {state_key} "
                f"original={original_session_id}, current={latest_session.session_id}"
            )
            return

        # Use the latest session data for archiving
        session = latest_session

        # Skip archiving if no segments generated
        if session.first_segment_number < 0 or session.last_segment_number < 0:
            logger.info(f"Session has no segments to archive (max duration): {state_key}")
            await self.session_store.restart_session(
                session.client_id, session.device_id, expected_session_id=session.session_id
            )
            return

        # Skip archiving if session too short
        if session.duration_seconds < self.config.min_session_duration_seconds:
            logger.info(
                f"Session too short to archive (max duration): {state_key} "
                f"({session.duration_seconds}s < {self.config.min_session_duration_seconds}s)"
            )
            await self.session_store.restart_session(
                session.client_id, session.device_id, expected_session_id=session.session_id
            )
            return

        logger.info(
            f"Breaking session for max duration: {state_key} session={session.session_id} "
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
            captured_started_at=session.captured_started_dt,
            captured_ended_at=session.captured_ended_dt,
        )

        # Archive first, then restart
        try:
            logger.info(
                f"Creating archive for max-duration session: {state_key} "
                f"session={session.session_id}"
            )
            await self.archive_service.create_archive(device_session)
        except Exception as e:
            logger.error(
                f"Error creating archive for {state_key} (max duration): {e}", exc_info=True
            )

        # Restart session even if archive fails - device is still active
        # Pass expected_session_id to prevent overwriting if another process already restarted
        await self.session_store.restart_session(
            session.client_id, session.device_id, expected_session_id=session.session_id
        )

    async def run_continuous(self) -> None:
        """
        Run the offline checker continuously.

        Checks for offline devices every check_interval seconds.
        Runs archive cleanup periodically (every cleanup_interval_checks checks).
        """
        if not self.config.enabled:
            logger.info("Archive/deferred transmissions disabled")
            return

        cleanup_interval_seconds = self.check_interval * self.cleanup_interval_checks

        logger.info("=" * 80)
        logger.info("Offline Checker Service Started (Continuous Mode)")
        logger.info(f"Offline threshold: {self.config.offline_threshold_seconds}s")
        logger.info(f"Min session duration: {self.config.min_session_duration_seconds}s")
        logger.info(
            f"Max session duration: {self.config.max_session_duration_seconds}s "
            f"({'disabled' if self.config.max_session_duration_seconds == 0 else 'enabled'})"
        )
        logger.info(f"Retention: {self.config.retention_days} days")
        logger.info(f"Check interval: {self.check_interval}s")
        logger.info(
            f"Archive cleanup interval: ~{cleanup_interval_seconds}s ({self.cleanup_interval_checks} checks)"
        )
        logger.info("=" * 80)

        self.running = True
        cleanup_counter = 0

        try:
            while self.running:
                try:
                    await self.check_once()
                except Exception as e:
                    logger.error(f"Error during offline check: {e}", exc_info=True)

                # Periodically cleanup expired archives
                cleanup_counter += 1
                if cleanup_counter >= self.cleanup_interval_checks:
                    cleanup_counter = 0
                    await self._run_archive_cleanup()

                await asyncio.sleep(self.check_interval)

        finally:
            await self.close()

    async def _run_archive_cleanup(self) -> None:
        """
        Run cleanup of expired archives.

        Deletes archive files from storage and marks database records as 'deleted'.
        """
        try:
            deleted = await self.archive_service.cleanup_expired_archives()
            if deleted > 0:
                logger.info(f"Archive cleanup: deleted {deleted} expired archive(s)")
            else:
                logger.debug("Archive cleanup: no expired archives to delete")
        except Exception as e:
            logger.error(f"Error during archive cleanup: {e}", exc_info=True)

    async def run_once(self) -> int:
        """
        Run the offline checker once and exit.

        Useful for Kubernetes CronJob deployments.
        Also runs archive cleanup to delete expired archives.

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
        logger.info(
            f"Max session duration: {self.config.max_session_duration_seconds}s "
            f"({'disabled' if self.config.max_session_duration_seconds == 0 else 'enabled'})"
        )
        logger.info(f"Retention: {self.config.retention_days} days")
        logger.info("=" * 80)

        try:
            count = await self.check_once()
            logger.info(f"Offline check complete: {count} session(s) processed")

            # Also run archive cleanup in one-shot mode
            await self._run_archive_cleanup()

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
