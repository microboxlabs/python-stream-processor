"""
Main entry point for the Stream Processor.

This service consumes frame events from Pulsar and generates HLS live streams
with 24-hour rolling windows per device.
"""

import sys
import os
import asyncio
import signal

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from stream_processor.consumer.pulsar_consumer import StreamProcessorConsumer
from stream_processor.service.cleanup_service import CleanupService
from stream_processor.utils.logger import get_logger
from stream_processor.utils.metrics import start_metrics_server

logger = get_logger(__name__)


async def shutdown(signal_received, consumer: StreamProcessorConsumer, cleanup: CleanupService):
    """Handle graceful shutdown."""
    logger.info(f"Received exit signal {signal_received.name}...")
    
    # Stop services
    await consumer.stop()
    await cleanup.stop()
    
    logger.info("Shutdown complete")


async def main_async():
    """Async main function to start all services."""
    logger.info("=" * 80)
    logger.info("Stream Processor - HLS Live Stream Generator")
    logger.info("=" * 80)
    
    # Start metrics server
    start_metrics_server()
    
    # Initialize services
    consumer = StreamProcessorConsumer()
    cleanup = CleanupService()
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s, consumer, cleanup))
        )
    
    # Start services
    logger.info("Starting services...")
    
    try:
        await asyncio.gather(
            consumer.run(),
            cleanup.run(),
        )
    except asyncio.CancelledError:
        logger.info("Services cancelled")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main entry point."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

