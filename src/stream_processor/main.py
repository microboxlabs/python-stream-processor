"""
Stream Processor CLI

Main entry points for the stream processor services:
- consumer: Process frames from Pulsar and generate HLS segments
- offline-checker: Detect offline devices and create archives
- cleanup: Clean up old HLS segments and frames (for CronJob use)
- archive-cleanup: Clean up expired archives (for CronJob use)
"""

import argparse
import asyncio
import signal
import sys

from .config.settings import settings
from .utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Stream Processor - HLS Live Stream Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  consumer         Run the Pulsar consumer for frame processing
  offline-checker  Run the offline detection service
  cleanup          Clean up old HLS segments and frames
  archive-cleanup  Clean up expired archives

Examples:
  stream-processor consumer
  stream-processor offline-checker --continuous
  stream-processor offline-checker --once
  stream-processor cleanup
  stream-processor archive-cleanup
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Consumer command
    consumer_parser = subparsers.add_parser(
        "consumer",
        help="Run the Pulsar consumer for frame processing",
    )
    consumer_parser.add_argument(
        "--no-redis",
        action="store_true",
        help="Disable Redis session tracking (use in-memory only)",
    )

    # Offline checker command
    checker_parser = subparsers.add_parser(
        "offline-checker",
        help="Run the offline detection service",
    )
    checker_parser.add_argument(
        "--continuous",
        action="store_true",
        default=True,
        help="Run continuously (default)",
    )
    checker_parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (for CronJob use)",
    )
    checker_parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Check interval in seconds (default: 10)",
    )

    # Cleanup command (for CronJob use)
    subparsers.add_parser(
        "cleanup",
        help="Clean up old HLS segments and frames (runs once, for CronJob use)",
    )

    args = parser.parse_args()

    if args.command is None:
        # Default to consumer for backwards compatibility
        args.command = "consumer"
        args.no_redis = False

    if args.command == "consumer":
        run_consumer(use_redis=not getattr(args, "no_redis", False))
    elif args.command == "offline-checker":
        continuous = not getattr(args, "once", False)
        interval = getattr(args, "interval", 10)
        run_offline_checker(continuous=continuous, interval=interval)
    elif args.command == "cleanup":
        run_cleanup()
    else:
        parser.print_help()
        sys.exit(1)


def run_consumer(use_redis: bool = True) -> None:
    """Run the Pulsar consumer."""
    from .consumer.pulsar_consumer import StreamProcessorConsumer
    from .utils.metrics import start_metrics_server

    # Start metrics server if enabled
    if settings.metrics.enabled:
        start_metrics_server()

    consumer = StreamProcessorConsumer(use_redis=use_redis)

    # Handle graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown_handler(sig: signal.Signals) -> None:
        logger.info(f"Received {sig.name}, initiating shutdown...")
        loop.create_task(consumer.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_handler, sig)

    try:
        loop.run_until_complete(consumer.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def run_offline_checker(continuous: bool = True, interval: int = 10) -> None:
    """Run the offline checker service."""
    from .service.offline_checker import OfflineChecker
    from .utils.metrics import start_metrics_server

    # Start metrics server if enabled
    if settings.metrics.enabled:
        start_metrics_server()

    checker = OfflineChecker(check_interval=interval)

    # Handle graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown_handler(sig: signal.Signals) -> None:
        logger.info(f"Received {sig.name}, initiating shutdown...")
        loop.create_task(checker.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_handler, sig)

    try:
        if continuous:
            loop.run_until_complete(checker.run_continuous())
        else:
            loop.run_until_complete(checker.run_once())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        loop.run_until_complete(checker.close())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def run_cleanup() -> None:
    """Run cleanup of old HLS segments and frames (one-shot, for CronJob use)."""
    from .service.cleanup_service import CleanupService

    logger.info("=" * 80)
    logger.info("Stream Processor - Cleanup (One-Shot Mode)")
    logger.info("=" * 80)

    cleanup = CleanupService()

    # Run cleanup once
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(cleanup._run_cleanup())
        logger.info("Cleanup complete")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Cleanup error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
