"""
Configuration management for the stream processor.
Uses pydantic-settings for environment variable loading with validation.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PulsarConfig(BaseSettings):
    """Pulsar connection configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PULSAR_", extra="ignore")

    service_url: str = Field(default="pulsar://localhost:6650", description="Pulsar broker URL")
    topic: str = Field(
        default="persistent://streamhub/stream/frames", description="Topic to consume from"
    )
    subscription: str = Field(default="stream-processor", description="Subscription name")
    consumer_name: str = Field(default="stream-processor-consumer", description="Consumer name")


class StorageConfig(BaseSettings):
    """Storage configuration supporting filesystem and GCS backends."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="STORAGE_", extra="ignore")

    # Storage backend type: 'filesystem' or 'gcs'
    type: str = Field(default="filesystem", description="Storage backend type (filesystem or gcs)")

    # Filesystem storage settings
    base_path: str = Field(
        default="/storage/streams", description="Base storage path for filesystem backend"
    )

    # GCS storage settings (used when type='gcs')
    gcs_bucket: str | None = Field(default=None, description="GCS bucket name")
    gcs_project_id: str | None = Field(
        default=None, description="GCS project ID (optional, uses ADC if not set)"
    )

    # Directory structure (same for both backends):
    # {base_path}/client_ids/{client_id}/device_id/{device_id}/frames/  <- actual frames
    # {base_path}/client_ids/{client_id}/device_id/{device_id}/hls/     <- HLS output
    # {base_path}/client_ids/{client_id}/request_id/{request_id}        -> symlink to ../device_id/{device_id}/frames
    # {base_path}/client_ids/{client_id}/secondary_key/{key}            -> symlink to ../device_id/{device_id}/frames


class ProcessingConfig(BaseSettings):
    """Video processing configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PROCESSING_", extra="ignore")

    max_workers: int = Field(default=50, description="Max concurrent FFmpeg workers")
    segment_duration_seconds: int = Field(default=30, description="HLS segment duration")
    frames_per_segment: int = Field(default=6, description="Frames per segment (at 5s intervals)")
    retention_hours: int = Field(default=24, description="Hours of video to retain")
    frame_interval_seconds: int = Field(default=5, description="Expected interval between frames")
    output_framerate: int = Field(default=1, description="Output video framerate")
    video_width: int = Field(default=1920, description="Output video width")


class RedisConfig(BaseSettings):
    """Redis configuration for distributed state."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="REDIS_", extra="ignore")

    url: str = Field(default="redis://localhost:6379", description="Redis connection URL")
    enabled: bool = Field(default=False, description="Enable Redis for distributed state")


class MetricsConfig(BaseSettings):
    """Prometheus metrics configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="METRICS_", extra="ignore")

    port: int = Field(default=9090, description="Metrics server port")
    enabled: bool = Field(default=True, description="Enable metrics endpoint")


class ArchiveConfig(BaseSettings):
    """Archive/deferred transmission configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="ARCHIVE_", extra="ignore")

    enabled: bool = Field(default=True, description="Enable deferred transmission archiving")
    retention_days: int = Field(default=7, description="Days to retain archived transmissions")
    min_session_duration_seconds: int = Field(
        default=60, description="Minimum session duration to archive"
    )
    offline_threshold_seconds: int = Field(
        default=60, description="Seconds without frames before considered offline"
    )
    max_session_duration_seconds: int = Field(
        default=7200,  # 2 hours
        description="Maximum session duration before auto-breaking (0 to disable)",
    )
    database_url: str | None = Field(
        default=None, description="PostgreSQL connection URL for archive metadata"
    )


class WatermarkConfig(BaseSettings):
    """Watermark configuration for timestamp overlays."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="WATERMARK_", extra="ignore")

    enabled: bool = Field(default=False, description="Enable timestamp watermarking")
    position: str = Field(
        default="top_right",
        description="Watermark position: top_right, top_left, bottom_right, bottom_left",
    )
    font_size: int = Field(default=24, description="Font size in pixels")
    format: str = Field(
        default="%Y-%m-%d %H:%M:%S.%f", description="Python strftime format string for timestamp"
    )
    timezone: str | None = Field(
        default=None,
        description="IANA timezone name (e.g., America/Santiago, UTC). If None, uses UTC.",
    )
    show_timezone: bool = Field(
        default=True, description="Show timezone offset and name in watermark"
    )


class Settings(BaseSettings):
    """Application settings container."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    pulsar: PulsarConfig = Field(default_factory=PulsarConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    archive: ArchiveConfig = Field(default_factory=ArchiveConfig)
    watermark: WatermarkConfig = Field(default_factory=WatermarkConfig)

    # Convenience properties
    @property
    def segments_per_24h(self) -> int:
        """Calculate number of segments in 24 hours."""
        return (self.processing.retention_hours * 3600) // self.processing.segment_duration_seconds


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()
