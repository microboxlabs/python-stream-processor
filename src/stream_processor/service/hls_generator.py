"""
HLS Generator Service

Generates HLS segments and maintains rolling playlists using FFmpeg.
"""

import os
import subprocess
import tempfile
from pathlib import Path

from ..config.settings import settings
from ..utils.logger import get_logger
from ..utils.metrics import ffmpeg_duration_histogram, segments_generated_total

logger = get_logger(__name__)


class HLSGenerator:
    """
    FFmpeg-based HLS segment generator.

    Creates video segments and maintains rolling playlists per device.
    """

    def __init__(self):
        """Initialize the HLS generator."""
        self.config = settings.processing
        self.storage = settings.storage

        # Base path will be created per client/device as needed
        Path(self.storage.base_path).mkdir(parents=True, exist_ok=True)

    def _get_device_hls_path(self, client_id: str, device_id: str) -> Path:
        """
        Get HLS output path for a client/device.

        Path structure: {base_path}/client_ids/{client_id}/device_id/{device_id}/hls/
        """
        device_path = (
            Path(self.storage.base_path) / "client_ids" / client_id /
            "device_id" / device_id / "hls"
        )
        device_path.mkdir(parents=True, exist_ok=True)

        segments_path = device_path / "segments"
        segments_path.mkdir(parents=True, exist_ok=True)

        return device_path

    def _create_input_file_list(self, frames: list[str]) -> str:
        """
        Create FFmpeg input file list for concat demuxer.

        Returns path to temporary file listing all input frames.
        """
        # Create temporary file with frame list
        fd, list_path = tempfile.mkstemp(suffix=".txt", prefix="ffmpeg_input_")

        try:
            with os.fdopen(fd, 'w') as f:
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

    def generate_segment(
        self,
        client_id: str,
        device_id: str,
        frames: list[str],
        segment_number: int,
    ) -> str:
        """
        Generate a single HLS segment from frames.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            frames: List of frame paths to include
            segment_number: Segment sequence number

        Returns:
            Path to generated segment file
        """
        import time
        start_time = time.time()

        device_path = self._get_device_hls_path(client_id, device_id)
        segment_filename = f"seg_{segment_number:06d}.ts"
        segment_path = device_path / "segments" / segment_filename

        logger.info(f"Generating segment {segment_filename} for {device_id}")
        logger.debug(f"Input frames: {len(frames)}")

        # Create input file list
        input_list_path = self._create_input_file_list(frames)

        try:
            # FFmpeg command to generate segment
            # Using concat demuxer for frame sequence
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-f", "concat",
                "-safe", "0",
                "-i", input_list_path,
                # Video encoding
                "-c:v", "libx264",
                "-preset", "ultrafast",  # Fast encoding for real-time
                "-tune", "zerolatency",
                "-profile:v", "baseline",  # Maximum compatibility
                "-level", "3.0",
                # Video settings
                "-vf", f"scale={self.config.video_width}:-2",  # Scale to width, keep aspect
                "-pix_fmt", "yuv420p",
                "-r", str(self.config.output_framerate),  # Output framerate
                # MPEG-TS output
                "-f", "mpegts",
                "-mpegts_copyts", "1",
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

            # Record metrics
            segments_generated_total.labels(device_id=device_id).inc()
            ffmpeg_duration_histogram.labels(device_id=device_id).observe(duration)

            logger.info(
                f"Segment generated: {segment_filename} "
                f"({segment_size} bytes, {duration:.2f}s)"
            )

            # Update playlist
            self._update_playlist(client_id, device_id, segment_number, segment_filename)

            return str(segment_path)

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
        device_path = self._get_device_hls_path(client_id, device_id)
        playlist_path = device_path / "playlist.m3u8"

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

        # Add segment entries
        for seg_num in range(oldest_segment, current_segment + 1):
            seg_filename = f"seg_{seg_num:06d}.ts"
            seg_path = device_path / "segments" / seg_filename

            if seg_path.exists():
                playlist_lines.append(f"#EXTINF:{segment_duration}.0,")
                playlist_lines.append(f"segments/{seg_filename}")

        # Write playlist
        playlist_content = "\n".join(playlist_lines) + "\n"

        # Atomic write
        temp_path = playlist_path.with_suffix(".tmp")
        temp_path.write_text(playlist_content)
        temp_path.rename(playlist_path)

        logger.debug(f"Playlist updated for {device_id}: {current_segment - oldest_segment + 1} segments")

    def get_playlist_path(self, client_id: str, device_id: str) -> Path:
        """Get the playlist path for a client/device."""
        return self._get_device_hls_path(client_id, device_id) / "playlist.m3u8"

    def get_segment_path(self, client_id: str, device_id: str, segment_filename: str) -> Path:
        """Get a segment file path for a client/device."""
        return self._get_device_hls_path(client_id, device_id) / "segments" / segment_filename

