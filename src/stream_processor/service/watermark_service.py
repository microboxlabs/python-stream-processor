"""
Watermark service for adding timestamp overlays to video frames.
"""

import asyncio
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from stream_processor.config.settings import WatermarkConfig, settings
from stream_processor.service.storage_backend import create_storage_backend


class WatermarkService:
    """Service for adding timestamp watermarks to frames."""

    def __init__(self, config: WatermarkConfig):
        """Initialize watermark service with configuration."""
        self.config = config
        self._font = None
        self.storage = create_storage_backend(
            storage_type=settings.storage.type,
            base_path=settings.storage.base_path,
            gcs_bucket=settings.storage.gcs_bucket,
            gcs_project_id=settings.storage.gcs_project_id,
        )
        # Dedicated dir for watermarked GCS frames. We write the watermarked image
        # to a local temp here and hand FFmpeg that path directly, instead of
        # re-uploading to GCS and re-downloading in the encoder. The consumer
        # deletes these after the segment is generated.
        self.wm_dir = Path(tempfile.gettempdir()) / "stream_wm"
        self.wm_dir.mkdir(parents=True, exist_ok=True)

    def is_watermark_temp(self, path: str) -> bool:
        """True if path is a watermarked temp this service produced (safe to delete)."""
        return not path.startswith("gs://") and path.startswith(str(self.wm_dir))

    def cleanup_temp_frames(self, paths: list[str]) -> None:
        """Delete watermarked temp frames (no-op for originals / GCS URIs)."""
        for path in paths:
            if self.is_watermark_temp(path):
                try:
                    Path(path).unlink(missing_ok=True)
                except OSError:
                    pass

    def _get_font(self, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        """Get font for watermark text."""
        try:
            # Try to use a monospace font for better readability
            return ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", size)
        except OSError:
            try:
                # Fallback to DejaVu Sans Mono (common on Linux)
                return ImageFont.truetype(
                    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", size
                )
            except OSError:
                # Use default font as last resort
                return ImageFont.load_default()

    def _get_position(
        self, image_width: int, image_height: int, text_bbox: tuple
    ) -> tuple[int, int]:
        """Calculate watermark position based on configuration."""
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        padding = 10

        position_map = {
            "top_right": (image_width - text_width - padding, padding),
            "top_left": (padding, padding),
            "bottom_right": (
                image_width - text_width - padding,
                image_height - text_height - padding,
            ),
            "bottom_left": (padding, image_height - text_height - padding),
        }

        return position_map.get(self.config.position, position_map["top_right"])

    def _format_timestamp(self, timestamp: datetime) -> str:
        """Format timestamp according to configuration with timezone support."""
        from zoneinfo import ZoneInfo

        # Track the effective timezone name for display
        effective_tz_name = None

        # Convert to configured timezone if specified
        if self.config.timezone:
            try:
                tz = ZoneInfo(self.config.timezone)
                timestamp = timestamp.astimezone(tz)
                effective_tz_name = self.config.timezone
            except Exception as e:
                # Fall back to UTC if timezone is invalid
                from ..utils.logger import get_logger

                logger = get_logger(__name__)
                logger.warning(f"Invalid timezone '{self.config.timezone}': {e}. Using UTC.")
                timestamp = timestamp.astimezone(ZoneInfo("UTC"))
                effective_tz_name = "UTC"
        else:
            # Default to UTC if no timezone configured
            from zoneinfo import ZoneInfo

            timestamp = timestamp.astimezone(ZoneInfo("UTC"))
            effective_tz_name = "UTC"

        # Format the timestamp
        formatted = timestamp.strftime(self.config.format)

        # Truncate microseconds to milliseconds for display
        if "%f" in self.config.format:
            # Get the actual microsecond value from the timestamp
            microsecond = timestamp.microsecond
            millisecond = microsecond // 1000
            # Replace the 6-digit microsecond string with 3-digit millisecond
            # This avoids accidentally matching other digit sequences like YYYYMMDD
            formatted = formatted.replace(f"{microsecond:06d}", f"{millisecond:03d}", 1)

        # Add timezone information if enabled
        if self.config.show_timezone:
            # Get UTC offset (e.g., -03:00)
            offset = timestamp.strftime("%z")
            # Format offset as ±HH:MM
            if offset:
                offset_formatted = f"{offset[:3]}:{offset[3:]}"
            else:
                offset_formatted = "+00:00"

            # Use the effective timezone name (matches the actual timezone used)
            tz_name = effective_tz_name or "UTC"

            # Append timezone info: "YYYY-MM-DD HH:MM:SS.sss -03:00 [America/Santiago]"
            formatted = f"{formatted} {offset_formatted} [{tz_name}]"

        return formatted

    async def add_timestamp_watermark(
        self,
        frame_path: str | Path,
        timestamp: datetime,
        output_path: str | Path | None = None,
    ) -> str:
        """
        Add timestamp watermark to a frame.

        Args:
            frame_path: Path to input frame image
            timestamp: Timestamp to display in watermark
            output_path: Optional output path (defaults to overwriting input)

        Returns:
            Path to watermarked frame
        """
        # Run image processing in thread pool to avoid blocking
        return await asyncio.to_thread(self._add_watermark_sync, frame_path, timestamp, output_path)

    def _add_watermark_sync(
        self,
        frame_path: str | Path,
        timestamp: datetime,
        output_path: str | Path | None = None,
    ) -> str:
        """Synchronous watermark implementation."""
        frame_path_str = str(frame_path)

        # Check if this is a GCS path
        if frame_path_str.startswith("gs://"):
            return self._add_watermark_gcs(frame_path_str, timestamp)

        # Handle filesystem paths
        frame_path = Path(frame_path)
        if output_path is None:
            output_path = frame_path
        else:
            output_path = Path(output_path)

        # Open image with context manager to ensure proper cleanup
        with Image.open(frame_path) as image:
            self._draw_watermark(image, timestamp)
            image.save(output_path, quality=95)

        return str(output_path)

    def _add_watermark_gcs(self, gcs_path: str, timestamp: datetime) -> str:
        """
        Apply a watermark to a GCS-stored frame and return a LOCAL temp path.

        Downloads the frame once, draws the timestamp, and writes the result to a
        local temp file under ``self.wm_dir`` — it does NOT upload back to GCS.
        FFmpeg then reads this local file directly, so each frame costs a single
        GCS download instead of download + re-upload + re-download. The original
        GCS object is left untouched. The consumer removes the temp via
        :meth:`cleanup_temp_frames` once the segment is generated.
        """
        import re

        from ..utils.logger import get_logger

        logger = get_logger(__name__)

        # Parse GCS path: gs://bucket/client_ids/{client_id}/device_id/{device_id}/frames/{filename}
        match = re.match(r"gs://[^/]+/client_ids/([^/]+)/device_id/([^/]+)/(.+)", gcs_path)

        if not match:
            raise ValueError(f"Invalid GCS path format: {gcs_path}")

        client_id = match.group(1)
        device_id = match.group(2)
        subpath = match.group(3)

        # Download the file from GCS
        file_data = self.storage.read_file(client_id, device_id, subpath)
        if file_data is None:
            raise FileNotFoundError(f"Frame not found in storage: {gcs_path}")

        tmp_in_path: Path | None = None
        out_path = self.wm_dir / f"wm_{uuid.uuid4().hex}.jpg"

        try:
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_in:
                tmp_in.write(file_data)
                tmp_in_path = Path(tmp_in.name)

            with Image.open(tmp_in_path) as image:
                self._draw_watermark(image, timestamp)
                image.save(out_path, quality=95)

            logger.debug(f"Watermarked frame written locally: {out_path} (from {gcs_path})")
            return str(out_path)

        finally:
            # Clean up only the downloaded input; the watermarked output is the
            # return value and is cleaned by the consumer after segment generation.
            if tmp_in_path and tmp_in_path.exists():
                try:
                    tmp_in_path.unlink()
                except OSError:
                    pass

    def _draw_watermark(self, image: Image.Image, timestamp: datetime) -> None:
        """Draw the timestamp overlay onto an open PIL image (in place)."""
        draw = ImageDraw.Draw(image, mode="RGBA")
        font = self._get_font(self.config.font_size)
        text = self._format_timestamp(timestamp)

        text_bbox = draw.textbbox((0, 0), text, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        x, y = self._get_position(image.width, image.height, text_bbox)

        bg_padding = 5
        bg_rect = [
            x - bg_padding,
            y - bg_padding,
            x + text_width + bg_padding,
            y + text_height + bg_padding,
        ]
        draw.rectangle(bg_rect, fill=(0, 0, 0, 204))  # 80% opacity black
        draw.text((x, y), text, fill=(255, 255, 255, 255), font=font)
