"""
HLS Generator Service

Generates HLS segments and maintains rolling playlists using FFmpeg.
Supports both filesystem and GCS storage backends.
"""

import os
import subprocess
import tempfile
from pathlib import Path

from ..config.settings import settings
from ..utils.logger import get_logger
from ..utils.metrics import ffmpeg_duration_histogram, segments_generated_total
from .storage_backend import (
    GcsStorageBackend,
    StorageBackend,
    create_storage_backend,
    download_gcs_uri,
)

logger = get_logger(__name__)


class HLSGenerator:
    """
    FFmpeg-based HLS segment generator.

    Creates video segments and maintains rolling playlists per device.
    Supports both filesystem and GCS storage backends.
    """

    def __init__(self, storage: StorageBackend | None = None):
        """
        Initialize the HLS generator.

        Args:
            storage: Optional storage backend. If not provided, creates one from settings.
        """
        self.config = settings.processing
        self.storage_config = settings.storage

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

        logger.info(f"HLS Generator using {self.storage.get_storage_type()} storage backend")

    def _ensure_hls_directories(self, client_id: str, device_id: str) -> None:
        """Ensure HLS directories exist for a device."""
        self.storage.ensure_directory_exists(client_id, device_id, "hls")
        self.storage.ensure_directory_exists(client_id, device_id, "hls/segments")

    def get_highest_segment_number(self, client_id: str, device_id: str) -> int:
        """
        Find the highest existing segment number by checking segment files.

        Used to resume segment numbering after restart.

        Returns:
            Highest segment number found, or -1 if no segments exist
        """
        import re

        self._ensure_hls_directories(client_id, device_id)

        # List all segment files
        segment_files = list(
            self.storage.list_files(client_id, device_id, "hls/segments", pattern="seg_*.ts")
        )

        if not segment_files:
            return -1

        # Find the most recently modified segment file
        latest_file = max(segment_files, key=lambda f: f.mtime)

        # Extract segment number from filename: seg_000123.ts -> 123
        match = re.search(r"seg_(\d+)\.ts$", latest_file.name)
        if not match:
            return -1

        highest = int(match.group(1))

        logger.info(
            f"Found existing segments for {client_id}:{device_id}, "
            f"latest: {latest_file.name}, resuming from segment {highest + 1}"
        )

        return highest

    def _create_input_file_list(self, frames: list[str]) -> str:
        """
        Create FFmpeg input file list for concat demuxer.

        Returns path to temporary file listing all input frames.
        """
        # Create temporary file with frame list
        fd, list_path = tempfile.mkstemp(suffix=".txt", prefix="ffmpeg_input_")

        try:
            with os.fdopen(fd, "w") as f:
                for frame_path in sorted(frames):
                    # Each frame should display for frame_interval_seconds
                    f.write(f"file '{frame_path}'\n")
                    f.write(f"duration {self.config.frame_interval_seconds}\n")

                # FFmpeg concat requires repeating last file for duration
                if frames:
                    f.write(f"file '{sorted(frames)[-1]}'\n")

        except Exception:
            os.unlink(list_path)
            raise

        return list_path

    def _prepare_frame_paths_for_ffmpeg(
        self, frames: list[str]
    ) -> tuple[list[str], list[str]]:
        """
        Prepare frame paths for FFmpeg processing.

        For filesystem backend, frames are already local.
        For GCS backend, downloads frames to local temp directory.

        Args:
            frames: List of frame paths (may be local or GCS URIs)

        Returns:
            Tuple of (local_paths, downloaded_paths) where:
            - local_paths: List of local paths that FFmpeg can read
            - downloaded_paths: List of paths that were downloaded from GCS (for cleanup)
        """
        local_paths = []
        downloaded_paths = []

        for frame_path in frames:
            if frame_path.startswith("gs://"):
                # GCS URI - download to temp location for FFmpeg
                if isinstance(self.storage, GcsStorageBackend):
                    # Use the storage backend's temp directory
                    local_path = self.storage.download_gcs_uri_to_local(frame_path)
                else:
                    # Filesystem storage but receiving GCS URIs from publisher
                    local_path = download_gcs_uri(frame_path)

                if local_path is not None:
                    local_path_str = str(local_path)
                    local_paths.append(local_path_str)
                    downloaded_paths.append(local_path_str)
                else:
                    logger.warning(f"Failed to download GCS frame: {frame_path}")
            else:
                # Local filesystem path
                local_paths.append(frame_path)

        return local_paths, downloaded_paths

    def generate_segment(
        self,
        client_id: str,
        device_id: str,
        frames: list[str],
        segment_number: int,
    ) -> str | None:
        """
        Generate a single HLS segment from frames.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            frames: List of frame paths to include
            segment_number: Segment sequence number

        Returns:
            Path/URI to generated segment file, or None if frames are missing
        """
        import time

        start_time = time.time()

        self._ensure_hls_directories(client_id, device_id)

        segment_filename = f"seg_{segment_number:06d}.ts"

        logger.info(f"Generating segment {segment_filename} for {device_id}")

        # Prepare local paths for frames (downloads GCS frames if needed)
        local_frames, downloaded_gcs_frames = self._prepare_frame_paths_for_ffmpeg(frames)

        # Validate all frame paths exist before processing
        missing_frames = [frame for frame in local_frames if not Path(frame).exists()]
        if missing_frames:
            logger.debug(
                f"Skipping segment generation for {device_id}: "
                f"{len(missing_frames)}/{len(local_frames)} frames missing"
            )
            return None

        if not local_frames:
            logger.debug(f"No valid frames for segment generation: {device_id}")
            return None

        logger.debug(f"Input frames: {len(local_frames)}")

        # Create input file list
        input_list_path = self._create_input_file_list(local_frames)

        # Determine output path based on storage type
        if isinstance(self.storage, GcsStorageBackend):
            # For GCS, write to temp directory then upload
            output_dir = self.storage.get_local_directory(client_id, device_id, "hls/segments")
            segment_path = output_dir / segment_filename
        else:
            # For filesystem, write directly
            output_dir = self.storage.get_local_directory(client_id, device_id, "hls/segments")
            segment_path = output_dir / segment_filename

        try:
            # FFmpeg command to generate segment
            # Using concat demuxer for frame sequence
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                input_list_path,
                # Video encoding
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",  # Fast encoding for real-time
                "-tune",
                "zerolatency",
                "-profile:v",
                "baseline",  # Maximum compatibility
                "-level",
                "3.0",
                # Video settings
                "-vf",
                f"scale={self.config.video_width}:-2",  # Scale to width, keep aspect
                "-pix_fmt",
                "yuv420p",
                "-r",
                str(self.config.output_framerate),  # Output framerate
                # MPEG-TS output
                "-f",
                "mpegts",
                "-mpegts_copyts",
                "1",
                str(segment_path),
            ]

            logger.debug(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")

            subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=120,  # 2 minute timeout
            )

            # Verify output
            if not segment_path.exists():
                msg = f"FFmpeg succeeded but segment not found: {segment_path}"
                raise RuntimeError(msg)

            segment_size = segment_path.stat().st_size
            duration = time.time() - start_time

            # For GCS, upload the segment
            final_uri: str
            if isinstance(self.storage, GcsStorageBackend):
                final_uri = self.storage.sync_local_to_gcs(
                    client_id, device_id, "hls/segments", segment_filename
                )
            else:
                final_uri = str(segment_path)

            # Record metrics
            segments_generated_total.labels(device_id=device_id).inc()
            ffmpeg_duration_histogram.labels(device_id=device_id).observe(duration)

            logger.info(
                f"Segment generated: {segment_filename} ({segment_size} bytes, {duration:.2f}s)"
            )

            # Update playlist
            self._update_playlist(client_id, device_id, segment_number, segment_filename)

            return final_uri

        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg failed: {e.stderr}")
            raise RuntimeError(f"FFmpeg generation failed: {e.stderr}") from e

        except subprocess.TimeoutExpired as e:
            logger.error("FFmpeg timeout after 2 minutes")
            raise RuntimeError("FFmpeg timeout") from e

        finally:
            # Clean up input list file
            try:
                os.unlink(input_list_path)
            except Exception:
                pass

            # Clean up downloaded GCS frames to avoid accumulating temp files
            for downloaded_frame in downloaded_gcs_frames:
                try:
                    Path(downloaded_frame).unlink(missing_ok=True)
                except Exception:
                    pass

    def _update_playlist(
        self,
        client_id: str,
        device_id: str,
        current_segment: int,
        segment_filename: str,
    ) -> None:
        """
        Update the HLS playlist for a device.

        Maintains a rolling window playlist with the last N segments.
        """
        # Calculate segment duration
        segment_duration = self.config.segment_duration_seconds

        # Calculate how many segments to keep (24 hours worth)
        max_segments = settings.segments_per_24h

        # Calculate the oldest segment to include
        oldest_segment = max(0, current_segment - max_segments + 1)

        # Build playlist
        playlist_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{segment_duration}",
            f"#EXT-X-MEDIA-SEQUENCE:{oldest_segment}",
        ]

        # Add segment entries - check which segments actually exist
        for seg_num in range(oldest_segment, current_segment + 1):
            seg_filename = f"seg_{seg_num:06d}.ts"

            # Check if segment exists in storage
            if self.storage.file_exists(client_id, device_id, f"hls/segments/{seg_filename}"):
                playlist_lines.append(f"#EXTINF:{segment_duration}.0,")
                playlist_lines.append(f"segments/{seg_filename}")

        # Write playlist
        playlist_content = "\n".join(playlist_lines) + "\n"

        # Use atomic write
        self.storage.write_file_atomic(
            client_id,
            device_id,
            "hls/playlist.m3u8",
            playlist_content.encode("utf-8"),
            content_type="application/vnd.apple.mpegurl",
        )

        logger.debug(
            f"Playlist updated for {device_id}: {current_segment - oldest_segment + 1} segments"
        )

    def get_playlist_path(self, client_id: str, device_id: str) -> Path | None:
        """Get the playlist path for a client/device (filesystem only)."""
        return self.storage.get_local_path(client_id, device_id, "hls/playlist.m3u8")

    def get_segment_path(self, client_id: str, device_id: str, segment_filename: str) -> Path | None:
        """Get a segment file path for a client/device (filesystem only)."""
        return self.storage.get_local_path(client_id, device_id, f"hls/segments/{segment_filename}")
