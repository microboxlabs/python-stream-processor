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

    def _probe_local_duration(self, path: Path) -> float:
        """
        Read the playback duration of a local TS file via ffprobe.

        Returns 0.0 on failure; the caller treats that as "do not advance
        cumulative offset" so a probe error never silently corrupts the
        per-session timeline.
        """
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=nw=1:nk=1",
                    str(path),
                ],
                capture_output=True,
                check=True,
                timeout=10,
            )
            return float(result.stdout.decode().strip())
        except (subprocess.SubprocessError, ValueError) as e:
            logger.warning(f"Failed to probe segment duration for {path}: {e}")
            return 0.0

    def _create_input_file_list(self, frames: list[str]) -> str:
        """
        Create FFmpeg input file list for concat demuxer.

        Returns path to temporary file listing all input frames.
        """
        # Create temporary file with frame list
        fd, list_path = tempfile.mkstemp(suffix=".txt", prefix="ffmpeg_input_")

        try:
            with os.fdopen(fd, "w") as f:
                for frame_path in frames:
                    # Each frame should display for frame_interval_seconds
                    f.write(f"file '{frame_path}'\n")
                    f.write(f"duration {self.config.frame_interval_seconds}\n")

                # FFmpeg concat requires repeating last file for duration
                if frames:
                    f.write(f"file '{frames[-1]}'\n")

        except Exception:
            os.unlink(list_path)
            raise

        return list_path

    def _prepare_frame_paths_for_ffmpeg(self, frames: list[str]) -> tuple[list[str], list[str]]:
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
        output_ts_offset: float = 0.0,
    ) -> tuple[str, float] | None:
        """
        Generate a single HLS segment from frames.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            frames: List of frame paths to include
            segment_number: Segment sequence number
            output_ts_offset: PTS offset (seconds) to apply via ffmpeg
                `-output_ts_offset`. Pass the cumulative duration of
                previously-generated segments in this session so the new
                segment's timestamps continue the timeline; HLS players
                rely on this for seeking.

        Returns:
            Tuple of (final URI/path, actual media duration in seconds), or
            None if generation was skipped (e.g. missing frames). The
            duration is read back via ffprobe so the caller can advance
            its own cumulative offset for the next segment.
        """
        import time

        start_time = time.time()

        self._ensure_hls_directories(client_id, device_id)

        segment_filename = f"seg_{segment_number:06d}.ts"

        logger.info(f"Generating segment {segment_filename} for {device_id}")

        # Initialize cleanup targets so the finally block is safe on any early exit
        downloaded_gcs_frames: list[str] = []
        input_list_path: str | None = None

        try:
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
            for i, frame in enumerate(local_frames):
                logger.debug(f"  Frame {i}: {frame}")

            # Create input file list
            input_list_path = self._create_input_file_list(local_frames)

            # Debug: log input file contents
            with open(input_list_path) as f:
                logger.debug(f"FFmpeg input file contents:\n{f.read()}")

            # Determine output path based on storage type
            output_dir = self.storage.get_local_directory(client_id, device_id, "hls/segments")
            if output_dir is None:
                logger.error(f"Cannot get output directory for segments: {device_id}")
                return None
            segment_path = output_dir / segment_filename

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
                # Shift PTS so segments share a continuous timeline across
                # the session — required for VOD seeking once archived.
                "-output_ts_offset",
                f"{output_ts_offset:.6f}",
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
            wall_duration = time.time() - start_time

            # Probe the actual media duration so the caller can advance its
            # cumulative PTS offset for the next segment.
            media_duration = self._probe_local_duration(segment_path)

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
            ffmpeg_duration_histogram.labels(device_id=device_id).observe(wall_duration)

            logger.info(
                f"Segment generated: {segment_filename} "
                f"({segment_size} bytes, media={media_duration:.3f}s, "
                f"wall={wall_duration:.2f}s, ts_offset={output_ts_offset:.3f}s)"
            )

            # Update playlist
            self._update_playlist(client_id, device_id, segment_number, segment_filename)

            return final_uri, media_duration

        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg failed: {e.stderr}")
            raise RuntimeError(f"FFmpeg generation failed: {e.stderr}") from e

        except subprocess.TimeoutExpired as e:
            logger.error("FFmpeg timeout after 2 minutes")
            raise RuntimeError("FFmpeg timeout") from e

        finally:
            # Clean up input list file (may be None if we returned before creating it)
            if input_list_path is not None:
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

    @staticmethod
    def _parse_extinf(playlist_path: str) -> list[float]:
        """Read per-segment durations from an HLS muxer playlist's #EXTINF lines."""
        durations: list[float] = []
        with open(playlist_path) as f:
            for line in f:
                if line.startswith("#EXTINF:"):
                    durations.append(float(line[len("#EXTINF:") :].split(",")[0]))
        return durations

    def generate_segments_batch(
        self,
        client_id: str,
        device_id: str,
        frames: list[str],
        base_segment_number: int,
        output_ts_offset: float = 0.0,
    ) -> list[tuple[int, float]]:
        """
        Catch-up encoder: turn many frames into many HLS segments in ONE FFmpeg
        process via the HLS muxer (approach (a)). Far cheaper than one process
        per segment when draining a backlog, and lets a single device saturate
        the worker pool.

        Only COMPLETE segments are produced: ``frames`` is truncated to a
        multiple of ``frames_per_segment`` so every emitted segment is exactly
        ``segment_duration_seconds`` (the playlist declares a uniform EXTINF, so
        partial segments must not be emitted here). Segments are numbered
        ``base_segment_number, base_segment_number+1, ...`` via ``-start_number``.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            frames: Ordered frame paths (local or gs://). Truncated to a multiple
                of frames_per_segment; leftover frames are the caller's to keep.
            base_segment_number: Sequence number of the first produced segment.
            output_ts_offset: PTS offset (seconds) for the first segment so the
                batch continues the session timeline (archive/VOD seeking).

        Returns:
            ``[(segment_number, duration_seconds), ...]`` in order — one entry
            per produced segment — or ``[]`` if skipped (too few/missing frames).
        """
        import time

        start_time = time.time()

        frames_per_segment = self.config.frames_per_segment
        segment_duration = self.config.segment_duration_seconds

        if not frames or len(frames) < frames_per_segment:
            return []

        num_segments = len(frames) // frames_per_segment
        usable_frames = frames[: num_segments * frames_per_segment]

        self._ensure_hls_directories(client_id, device_id)

        downloaded_gcs_frames: list[str] = []
        input_list_path: str | None = None
        batch_playlist_path: str | None = None

        try:
            local_frames, downloaded_gcs_frames = self._prepare_frame_paths_for_ffmpeg(
                usable_frames
            )
            if len(local_frames) != len(usable_frames) or any(
                not Path(f).exists() for f in local_frames
            ):
                logger.warning(
                    f"Skipping batch for {device_id}: "
                    f"{len(usable_frames) - len(local_frames)} frames unresolved/missing"
                )
                return []

            input_list_path = self._create_input_file_list(local_frames)

            output_dir = self.storage.get_local_directory(client_id, device_id, "hls/segments")
            if output_dir is None:
                logger.error(f"Cannot get output directory for segments: {device_id}")
                return []

            total_seconds = num_segments * segment_duration
            fd, batch_playlist_path = tempfile.mkstemp(suffix=".m3u8", prefix="hls_batch_")
            os.close(fd)

            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                input_list_path,
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-profile:v",
                "baseline",
                "-level",
                "3.0",
                "-vf",
                f"scale={self.config.video_width}:-2",
                "-pix_fmt",
                "yuv420p",
                "-r",
                str(self.config.output_framerate),
                "-fps_mode",
                "cfr",
                # Encode exactly num_segments * segment_duration; drops the concat
                # tail frame so we emit only complete, uniform-length segments.
                "-t",
                str(total_seconds),
                # Force a keyframe at every segment boundary so the HLS muxer cuts
                # cleanly into exact segment_duration pieces.
                "-force_key_frames",
                f"expr:gte(t,n_forced*{segment_duration})",
                # Continue the session timeline for archive/VOD seeking.
                "-mpegts_copyts",
                "1",
                "-output_ts_offset",
                f"{output_ts_offset:.6f}",
                "-f",
                "hls",
                "-hls_time",
                str(segment_duration),
                "-hls_list_size",
                "0",
                "-hls_playlist_type",
                "vod",
                "-hls_flags",
                "independent_segments",
                "-hls_segment_type",
                "mpegts",
                "-start_number",
                str(base_segment_number),
                "-hls_segment_filename",
                str(output_dir / "seg_%06d.ts"),
                batch_playlist_path,
            ]

            logger.debug(f"FFmpeg batch command: {' '.join(ffmpeg_cmd)}")

            subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=max(120, num_segments * 10),
            )

            durations = self._parse_extinf(batch_playlist_path)
            wall_duration = time.time() - start_time
            wall_per_segment = wall_duration / num_segments

            results: list[tuple[int, float]] = []
            for i in range(num_segments):
                segment_number = base_segment_number + i
                segment_filename = f"seg_{segment_number:06d}.ts"
                segment_path = output_dir / segment_filename
                if not segment_path.exists():
                    logger.warning(f"Batch segment missing after encode: {segment_filename}")
                    continue

                if isinstance(self.storage, GcsStorageBackend):
                    self.storage.sync_local_to_gcs(
                        client_id, device_id, "hls/segments", segment_filename
                    )

                duration = durations[i] if i < len(durations) else float(segment_duration)
                segments_generated_total.labels(device_id=device_id).inc()
                ffmpeg_duration_histogram.labels(device_id=device_id).observe(wall_per_segment)
                results.append((segment_number, duration))

            if results:
                # Refresh the filesystem rolling playlist to the last produced segment.
                last_number = results[-1][0]
                self._update_playlist(
                    client_id, device_id, last_number, f"seg_{last_number:06d}.ts"
                )

            logger.info(
                f"Batch generated {len(results)} segments "
                f"[{base_segment_number}..{base_segment_number + num_segments - 1}] "
                f"for {device_id} (wall={wall_duration:.2f}s, "
                f"{len(results) / wall_duration:.1f} seg/s, ts_offset={output_ts_offset:.3f}s)"
            )
            return results

        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg batch failed: {e.stderr}")
            raise RuntimeError(f"FFmpeg batch generation failed: {e.stderr}") from e

        except subprocess.TimeoutExpired as e:
            logger.error("FFmpeg batch timeout")
            raise RuntimeError("FFmpeg batch timeout") from e

        finally:
            if input_list_path is not None:
                try:
                    os.unlink(input_list_path)
                except Exception:
                    pass
            if batch_playlist_path is not None:
                try:
                    os.unlink(batch_playlist_path)
                except Exception:
                    pass
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
        Note: We trust that segments in the range exist rather than checking
        each one via file_exists, which is expensive for GCS (one API call per segment).
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

        # Add segment entries for the range
        # We trust segments exist since we generated them sequentially
        # Missing segments (due to cleanup) will cause HLS player to skip gracefully
        for seg_num in range(oldest_segment, current_segment + 1):
            seg_filename = f"seg_{seg_num:06d}.ts"
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

    def get_segment_path(
        self, client_id: str, device_id: str, segment_filename: str
    ) -> Path | None:
        """Get a segment file path for a client/device (filesystem only)."""
        return self.storage.get_local_path(client_id, device_id, f"hls/segments/{segment_filename}")
