"""
Watermark service for adding timestamp overlays to video frames.
"""

import asyncio
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from stream_processor.config.settings import WatermarkConfig


class WatermarkService:
    """Service for adding timestamp watermarks to frames."""

    def __init__(self, config: WatermarkConfig):
        """Initialize watermark service with configuration."""
        self.config = config
        self._font = None

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
        """Format timestamp according to configuration."""
        import re

        formatted = timestamp.strftime(self.config.format)
        # Truncate microseconds to milliseconds for display
        if "%f" in self.config.format:
            # Replace the 6-digit microsecond with 3-digit millisecond
            # Matches 6 consecutive digits (microseconds) and replaces with first 3 digits
            formatted = re.sub(r"(\d{3})\d{3}", r"\1", formatted, count=1)
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
        frame_path = Path(frame_path)
        if output_path is None:
            output_path = frame_path
        else:
            output_path = Path(output_path)

        # Open image with context manager to ensure proper cleanup
        with Image.open(frame_path) as image:
            # Create drawing context
            draw = ImageDraw.Draw(image, mode="RGBA")

            # Get font
            font = self._get_font(self.config.font_size)

            # Format timestamp text
            text = self._format_timestamp(timestamp)

            # Get text bounding box
            text_bbox = draw.textbbox((0, 0), text, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            # Calculate position
            x, y = self._get_position(image.width, image.height, text_bbox)

            # Draw semi-transparent background
            bg_padding = 5
            bg_rect = [
                x - bg_padding,
                y - bg_padding,
                x + text_width + bg_padding,
                y + text_height + bg_padding,
            ]
            draw.rectangle(bg_rect, fill=(0, 0, 0, 204))  # 80% opacity black

            # Draw white text
            draw.text((x, y), text, fill=(255, 255, 255, 255), font=font)

            # Save image
            image.save(output_path, quality=95)

        return str(output_path)
