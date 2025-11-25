"""
Prometheus metrics for stream processor monitoring.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from ..config.settings import settings
from .logger import get_logger

logger = get_logger(__name__)


# Counters
frames_received_total = Counter(
    "stream_processor_frames_received_total",
    "Total number of frames received",
    ["device_id"],
)

segments_generated_total = Counter(
    "stream_processor_segments_generated_total",
    "Total number of HLS segments generated",
    ["device_id"],
)

segments_deleted_total = Counter(
    "stream_processor_segments_deleted_total",
    "Total number of HLS segments deleted during cleanup",
    ["device_id"],
)

processing_errors_total = Counter(
    "stream_processor_errors_total",
    "Total number of processing errors",
    ["device_id", "error_type"],
)


# Gauges
active_devices_gauge = Gauge(
    "stream_processor_active_devices",
    "Number of currently active devices",
)


# Histograms
ffmpeg_duration_histogram = Histogram(
    "stream_processor_ffmpeg_duration_seconds",
    "FFmpeg segment generation duration",
    ["device_id"],
    buckets=[0.5, 1, 2, 5, 10, 20, 30, 60, 120],
)

cleanup_duration_histogram = Histogram(
    "stream_processor_cleanup_duration_seconds",
    "Cleanup service duration",
    buckets=[1, 5, 10, 30, 60, 120, 300],
)


def start_metrics_server() -> None:
    """Start the Prometheus metrics HTTP server."""
    if not settings.metrics.enabled:
        logger.info("Metrics server disabled")
        return
    
    port = settings.metrics.port
    
    try:
        start_http_server(port)
        logger.info(f"Metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")

