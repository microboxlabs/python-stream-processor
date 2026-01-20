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
  reset-device     Reset all data for a specific device

Examples:
  stream-processor consumer
  stream-processor offline-checker --continuous
  stream-processor offline-checker --once
  stream-processor cleanup
  stream-processor archive-cleanup
  stream-processor reset-device --client-id abc123 --device-id device001
  stream-processor reset-device -c abc123 -d device001 --dry-run
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

    # Archive cleanup command (for CronJob use)
    subparsers.add_parser(
        "archive-cleanup",
        help="Clean up expired archives (runs once, for CronJob use)",
    )

    # Reset device command
    reset_parser = subparsers.add_parser(
        "reset-device",
        help="Reset all data for a specific device (Redis, storage, database)",
    )
    reset_parser.add_argument(
        "-c",
        "--client-id",
        required=True,
        help="Client identifier",
    )
    reset_parser.add_argument(
        "-d",
        "--device-id",
        required=True,
        help="Device identifier",
    )
    reset_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    reset_parser.add_argument(
        "--skip-redis",
        action="store_true",
        help="Skip Redis cleanup (segments, sessions)",
    )
    reset_parser.add_argument(
        "--skip-storage",
        action="store_true",
        help="Skip storage cleanup (frames, HLS files)",
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
    elif args.command == "archive-cleanup":
        run_archive_cleanup()
    elif args.command == "reset-device":
        run_reset_device(
            client_id=args.client_id,
            device_id=args.device_id,
            dry_run=getattr(args, "dry_run", False),
            skip_redis=getattr(args, "skip_redis", False),
            skip_storage=getattr(args, "skip_storage", False),
        )
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


def run_archive_cleanup() -> None:
    """Run cleanup of expired archives (one-shot, for CronJob use)."""
    from .service.archive_service import ArchiveService

    logger.info("=" * 80)
    logger.info("Stream Processor - Archive Cleanup (One-Shot Mode)")
    logger.info(f"Retention: {settings.archive.retention_days} days")
    logger.info("=" * 80)

    archive_service = ArchiveService()

    # Run archive cleanup once
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        deleted = loop.run_until_complete(archive_service.cleanup_expired_archives())
        if deleted > 0:
            logger.info(f"Archive cleanup complete: {deleted} expired archive(s) deleted")
        else:
            logger.info("Archive cleanup complete: no expired archives to delete")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Archive cleanup error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        loop.run_until_complete(archive_service.close())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def run_reset_device(
    client_id: str,
    device_id: str,
    dry_run: bool = False,
    skip_redis: bool = False,
    skip_storage: bool = False,
) -> None:
    """Reset all data for a specific device."""
    from .service.device_reset_service import DeviceResetService

    logger.info("=" * 80)
    logger.info("Stream Processor - Device Reset")
    logger.info(f"Client ID: {client_id}")
    logger.info(f"Device ID: {device_id}")
    logger.info(f"Dry Run: {dry_run}")
    logger.info(f"Skip Redis: {skip_redis}")
    logger.info(f"Skip Storage: {skip_storage}")
    logger.info("=" * 80)

    reset_service = DeviceResetService()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        result = loop.run_until_complete(
            reset_service.reset_device(
                client_id=client_id,
                device_id=device_id,
                dry_run=dry_run,
                skip_redis=skip_redis,
                skip_storage=skip_storage,
            )
        )

        logger.info("=" * 80)
        logger.info("Reset Summary:")
        for key, value in result.items():
            logger.info(f"  {key}: {value}")
        logger.info("=" * 80)

        if dry_run:
            logger.info("Dry run complete - no data was deleted")
        else:
            logger.info("Device reset complete")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Device reset error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        loop.run_until_complete(reset_service.close())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
