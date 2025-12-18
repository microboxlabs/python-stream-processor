"""
Archive Service

Creates deferred transmission archives when devices go offline.
Generates VOD playlists and stores archive metadata in PostgreSQL.
"""

from datetime import timedelta

import asyncpg

from ..config.settings import settings
from ..utils.logger import get_logger
from .session_tracker import DeviceSession
from .storage_backend import StorageBackend, create_storage_backend

logger = get_logger(__name__)


class ArchiveService:
    """
    Service for creating and managing deferred transmission archives.

    When a device goes offline, this service:
    1. Copies HLS segments to an archive location
    2. Generates a VOD playlist (with #EXT-X-ENDLIST)
    3. Stores metadata in PostgreSQL
    """

    def __init__(self, storage: StorageBackend | None = None):
        """
        Initialize the archive service.

        Args:
            storage: Optional storage backend. If not provided, creates one from settings.
        """
        self.config = settings.archive
        self.storage_config = settings.storage
        self.processing_config = settings.processing

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

        # Database pool (initialized lazily)
        self._db_pool: asyncpg.Pool | None = None

        logger.info(f"Archive Service initialized with {self.storage.get_storage_type()} backend")

    async def _get_db_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self._db_pool is None:
            if not self.config.database_url:
                raise ValueError(
                    "ARCHIVE_DATABASE_URL not configured. "
                    "Set this environment variable to enable archive creation."
                )
            self._db_pool = await asyncpg.create_pool(
                self.config.database_url,
                min_size=1,
                max_size=5,
            )
            logger.info("Database connection pool created")
        return self._db_pool

    async def create_archive(self, session: DeviceSession) -> str | None:
        """
        Create a deferred transmission archive from a completed session.

        1. Copy segments from live hls/segments/ to archives/{session_id}/segments/
        2. Generate VOD playlist
        3. Store metadata in database

        Args:
            session: The completed device session

        Returns:
            Archive session_id if successful, None otherwise
        """
        logger.info(
            f"Creating archive for {session.state_key}: "
            f"session={session.session_id}, "
            f"segments={session.first_segment_number}-{session.last_segment_number}, "
            f"duration={session.duration_seconds}s"
        )

        try:
            # Calculate archive path
            archive_subpath = f"archives/{session.session_id}"

            # 1. Copy segments to archive location
            copied_segments = await self._copy_segments(
                session.client_id,
                session.device_id,
                session.first_segment_number,
                session.last_segment_number,
                archive_subpath,
            )

            if not copied_segments:
                logger.warning(f"No segments copied for archive {session.session_id}")
                return None

            # 2. Generate VOD playlist using the list of copied segments
            await self._generate_vod_playlist(
                session.client_id,
                session.device_id,
                archive_subpath,
                copied_segments,
            )

            # 3. Store metadata in database
            await self._store_archive_metadata(session, archive_subpath)

            logger.info(
                f"Archive created successfully: {session.session_id} "
                f"({len(copied_segments)} segments)"
            )
            return session.session_id

        except Exception as e:
            logger.error(f"Failed to create archive {session.session_id}: {e}", exc_info=True)
            # Try to mark as failed in database
            try:
                await self._mark_archive_failed(session)
            except Exception:
                pass
            return None

    async def _copy_segments(
        self,
        client_id: str,
        device_id: str,
        first_segment: int,
        last_segment: int,
        archive_subpath: str,
    ) -> list[str]:
        """
        Copy segment files from live to archive location.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            first_segment: First segment number
            last_segment: Last segment number
            archive_subpath: Archive path (e.g., "archives/{session_id}")

        Returns:
            List of copied segment filenames
        """
        copied = []

        # Ensure archive directory exists
        self.storage.ensure_directory_exists(client_id, device_id, f"{archive_subpath}/segments")

        for seg_num in range(first_segment, last_segment + 1):
            seg_filename = f"seg_{seg_num:06d}.ts"
            src_subpath = f"hls/segments/{seg_filename}"
            dst_subpath = f"{archive_subpath}/segments/{seg_filename}"

            # Read from live location
            data = self.storage.read_file(client_id, device_id, src_subpath)
            if data:
                # Write to archive location
                self.storage.write_file(
                    client_id,
                    device_id,
                    dst_subpath,
                    data,
                    content_type="video/mp2t",
                )
                copied.append(seg_filename)
            else:
                logger.debug(f"Segment not found (may have been cleaned up): {src_subpath}")

        logger.debug(f"Copied {len(copied)} of {last_segment - first_segment + 1} segments")
        return copied

    async def _generate_vod_playlist(
        self,
        client_id: str,
        device_id: str,
        archive_subpath: str,
        copied_segments: list[str],
    ) -> None:
        """
        Generate a complete VOD playlist for the archive.

        VOD playlists differ from live playlists:
        - Include #EXT-X-PLAYLIST-TYPE:VOD
        - Include #EXT-X-ENDLIST at the end
        - Start media sequence from 0

        Args:
            client_id: Client identifier
            device_id: Device identifier
            archive_subpath: Archive path
            copied_segments: List of segment filenames that were copied
        """
        segment_duration = self.processing_config.segment_duration_seconds

        # Build VOD playlist
        playlist_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{segment_duration}",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]

        # Add all copied segments in order
        for seg_filename in sorted(copied_segments):
            playlist_lines.append(f"#EXTINF:{segment_duration}.0,")
            playlist_lines.append(f"segments/{seg_filename}")

        # End marker for VOD
        playlist_lines.append("#EXT-X-ENDLIST")

        playlist_content = "\n".join(playlist_lines) + "\n"

        # Write playlist atomically
        self.storage.write_file_atomic(
            client_id,
            device_id,
            f"{archive_subpath}/playlist.m3u8",
            playlist_content.encode("utf-8"),
            content_type="application/vnd.apple.mpegurl",
        )

        logger.debug(f"VOD playlist generated for archive {archive_subpath}")

    async def _store_archive_metadata(
        self,
        session: DeviceSession,
        archive_path: str,
    ) -> None:
        """
        Store archive metadata in PostgreSQL.

        Args:
            session: The device session
            archive_path: Path to the archive
        """
        pool = await self._get_db_pool()

        expires_at = session.last_frame_at + timedelta(days=self.config.retention_days)

        await pool.execute(
            """
            INSERT INTO deferred_transmissions (
                client_id, device_id, session_id, owner_client_id,
                started_at, ended_at, duration_seconds,
                first_segment_number, last_segment_number, segment_count,
                archive_path, status, expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'ready', $12)
            """,
            session.client_id,
            session.device_id,
            session.session_id,
            session.client_id,  # owner_client_id = client_id (device is directly connected)
            session.started_at,
            session.last_frame_at,
            session.duration_seconds,
            session.first_segment_number,
            session.last_segment_number,
            session.segment_count,
            archive_path,
            expires_at,
        )

        logger.debug(f"Archive metadata stored: {session.session_id}")

    async def _mark_archive_failed(self, session: DeviceSession) -> None:
        """
        Mark an archive as failed in the database.

        Args:
            session: The device session
        """
        try:
            pool = await self._get_db_pool()
            await pool.execute(
                """
                INSERT INTO deferred_transmissions (
                    client_id, device_id, session_id, owner_client_id,
                    started_at, ended_at, duration_seconds,
                    first_segment_number, last_segment_number, segment_count,
                    archive_path, status, expires_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'failed', $12)
                ON CONFLICT (client_id, device_id, session_id)
                DO UPDATE SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                """,
                session.client_id,
                session.device_id,
                session.session_id,
                session.client_id,  # owner_client_id = client_id (device is directly connected)
                session.started_at,
                session.last_frame_at,
                session.duration_seconds,
                session.first_segment_number,
                session.last_segment_number,
                session.segment_count,
                f"archives/{session.session_id}",
                session.last_frame_at + timedelta(days=self.config.retention_days),
            )
        except Exception as e:
            logger.error(f"Failed to mark archive as failed: {e}")

    async def cleanup_expired_archives(self) -> int:
        """
        Delete archives that have exceeded retention period.

        Returns:
            Number of archives deleted
        """
        try:
            pool = await self._get_db_pool()
        except ValueError:
            # Database not configured
            return 0

        # Find expired archives
        rows = await pool.fetch(
            """
            SELECT id, client_id, device_id, session_id, archive_path
            FROM deferred_transmissions
            WHERE status = 'ready' AND expires_at < CURRENT_TIMESTAMP
            """
        )

        deleted_count = 0

        for row in rows:
            try:
                archive_path = row["archive_path"]

                # Delete segments
                for file_info in self.storage.list_files(
                    row["client_id"],
                    row["device_id"],
                    f"{archive_path}/segments",
                    pattern="*.ts",
                ):
                    self.storage.delete_file(
                        row["client_id"],
                        row["device_id"],
                        f"{archive_path}/segments/{file_info.name}",
                    )

                # Delete playlist
                self.storage.delete_file(
                    row["client_id"],
                    row["device_id"],
                    f"{archive_path}/playlist.m3u8",
                )

                # Mark as deleted in database
                await pool.execute(
                    """
                    UPDATE deferred_transmissions
                    SET status = 'deleted', updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                    """,
                    row["id"],
                )

                deleted_count += 1
                logger.info(f"Deleted expired archive: {row['session_id']}")

            except Exception as e:
                logger.error(f"Error deleting archive {row['session_id']}: {e}")

        return deleted_count

    async def close(self) -> None:
        """Close database connections."""
        if self._db_pool:
            await self._db_pool.close()
            self._db_pool = None
            logger.info("Database connection pool closed")
