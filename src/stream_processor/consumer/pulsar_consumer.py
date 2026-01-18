"""
Pulsar consumer for frame events with Key_Shared subscription.
Processes frames and triggers HLS segment generation.
"""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import pulsar

from ..config.settings import settings
from ..model.events import DeviceState, FrameEvent
from ..service.hls_generator import HLSGenerator
from ..service.redis_playlist_store import RedisPlaylistStore
from ..service.redis_session_store import RedisSessionStore
from ..service.storage_backend import sanitize_path_component
from ..service.watermark_service import WatermarkService
from ..utils.logger import get_log_level, get_logger
from ..utils.metrics import (
    active_devices_gauge,
    frames_received_total,
    processing_errors_total,
)

logger = get_logger(__name__)


class StreamProcessorConsumer:
    """
    Pulsar consumer for stream processing.

    Uses Key_Shared subscription to ensure ordering per device
    while allowing horizontal scaling across multiple instances.
    """

    def __init__(self, use_redis: bool = True):
        """
        Initialize the consumer.

        Args:
            use_redis: If True and Redis is enabled, use Redis for session tracking.
                      Session tracking is used by the separate offline-checker service.
        """
        self.config = settings.pulsar
        self.processing_config = settings.processing
        self.archive_config = settings.archive
        self.watermark_config = settings.watermark

        # Device state tracking (in-memory)
        self.device_states: dict[str, DeviceState] = {}

        # HLS generator service
        self.hls_generator = HLSGenerator()

        # Watermark service (only initialized if enabled)
        self.watermark_service: WatermarkService | None = None
        if self.watermark_config.enabled:
            self.watermark_service = WatermarkService(self.watermark_config)
            logger.info("Watermark service enabled")

        # Thread pool for FFmpeg workers
        self.executor = ThreadPoolExecutor(
            max_workers=self.processing_config.max_workers, thread_name_prefix="ffmpeg-worker-"
        )

        # Pulsar client and consumer (initialized on run)
        self.client: pulsar.Client | None = None
        self.consumer: pulsar.Consumer | None = None
        self.running = False

        # Segment generation lock per device
        self.device_locks: dict[str, asyncio.Lock] = {}

        # Redis session store for distributed offline detection
        # The offline-checker service reads this to detect offline devices
        self.session_store: RedisSessionStore | None = None

        if use_redis and settings.redis.enabled and self.archive_config.enabled:
            self.session_store = RedisSessionStore()
            logger.info("Redis session tracking enabled for offline detection")

        # Redis playlist store for dynamic playlist generation
        # Stores segment metadata for on-the-fly playlist generation by quarkus
        self.playlist_store: RedisPlaylistStore | None = None

        if use_redis and settings.redis.enabled and settings.redis.playlist_enabled:
            self.playlist_store = RedisPlaylistStore()
            logger.info("Redis playlist store enabled for dynamic playlist generation")

    def _get_or_create_state(self, client_id: str, device_id: str) -> DeviceState:
        """Get or create device state using client_id:device_id key."""
        state_key = f"{client_id}:{device_id}"
        if state_key not in self.device_states:
            # Check for existing segments to resume numbering
            highest_segment = self.hls_generator.get_highest_segment_number(client_id, device_id)
            initial_segment = highest_segment + 1 if highest_segment >= 0 else 0

            self.device_states[state_key] = DeviceState(
                client_id=client_id,
                device_id=device_id,
                current_segment_number=initial_segment,
            )
            active_devices_gauge.inc()
            logger.info(
                f"New device registered: {state_key} (starting at segment {initial_segment})"
            )
        return self.device_states[state_key]

    def _get_device_lock(self, state_key: str) -> asyncio.Lock:
        """Get or create async lock for device using state_key."""
        if state_key not in self.device_locks:
            self.device_locks[state_key] = asyncio.Lock()
        return self.device_locks[state_key]

    async def _process_frame(self, event: FrameEvent) -> None:
        """
        Process a single frame event.

        Accumulates frames and triggers segment generation when threshold is reached.
        """
        client_id = event.client_id
        device_id = sanitize_path_component(event.device_id)
        state_key = f"{client_id}:{device_id}"
        lock = self._get_device_lock(state_key)

        # Apply watermark if enabled
        frame_path = event.frame_path
        if self.watermark_service and event.request_timestamp:
            try:
                # Use request_timestamp if available, otherwise fall back to timestamp
                timestamp = event.request_timestamp
                frame_path = await self.watermark_service.add_timestamp_watermark(
                    event.frame_path, timestamp
                )
                logger.debug(f"Watermark applied to {frame_path}")
            except Exception as e:
                logger.error(f"Failed to apply watermark to {event.frame_path}: {e}", exc_info=True)
                # Continue with original frame path if watermarking fails
                frame_path = event.frame_path

        async with lock:
            state = self._get_or_create_state(client_id, device_id)

            # Add frame to pending
            state.add_frame(frame_path, event.timestamp)
            frames_received_total.labels(device_id=state_key).inc()

            # Update session activity in Redis (for offline-checker service)
            if self.session_store:
                await self.session_store.update_activity(client_id, device_id)

            logger.debug(
                f"Frame received for {state_key}: {frame_path} (pending: {state.frame_count})"
            )

            # Check if we should generate a segment
            if state.should_generate_segment(
                self.processing_config.frames_per_segment,
                max_wait_seconds=self.processing_config.segment_duration_seconds,
            ):
                await self._generate_segment(state)

    async def _generate_segment(self, state: DeviceState) -> None:
        """Generate HLS segment from accumulated frames."""
        client_id = state.client_id
        device_id = state.device_id
        state_key = state.state_key
        frames = state.pending_frames.copy()
        segment_number = state.current_segment_number

        if not frames:
            logger.warning(f"No frames to process for {state_key}")
            return

        logger.info(f"Generating segment {segment_number} for {state_key} ({len(frames)} frames)")

        try:
            # Run FFmpeg in thread pool
            loop = asyncio.get_event_loop()
            segment_path = await loop.run_in_executor(
                self.executor,
                self.hls_generator.generate_segment,
                client_id,
                device_id,
                frames,
                segment_number,
            )

            # Clear pending frames (even if generation was skipped)
            state.clear_pending_frames()

            if segment_path:
                logger.info(f"Segment generated: {segment_path}")

                # Update session segment info in Redis (for offline-checker service)
                if self.session_store:
                    await self.session_store.update_segment(client_id, device_id, segment_number)

                # Add segment to playlist store in Redis (for dynamic playlist generation)
                if self.playlist_store:
                    await self.playlist_store.add_segment(client_id, device_id, segment_number)
            else:
                logger.debug(f"Segment generation skipped for {state_key} (missing frames)")

        except Exception as e:
            processing_errors_total.labels(device_id=state_key, error_type="ffmpeg").inc()
            logger.error(f"Failed to generate segment for {state_key}: {e}", exc_info=True)

    async def _handle_message(self, msg) -> None:
        """Handle a single Pulsar message."""
        if self.consumer is None:
            logger.error("Consumer not initialized")
            return

        try:
            # Parse message
            data = json.loads(msg.data().decode("utf-8"))
            event = FrameEvent.model_validate(data)

            # Process the frame
            await self._process_frame(event)

            # Acknowledge the message
            self.consumer.acknowledge(msg)

        except json.JSONDecodeError as e:
            processing_errors_total.labels(device_id="unknown", error_type="json_parse").inc()
            logger.error(f"Failed to parse message: {e}")
            self.consumer.negative_acknowledge(msg)

        except Exception as e:
            processing_errors_total.labels(device_id="unknown", error_type="processing").inc()
            logger.error(f"Error processing message: {e}", exc_info=True)
            self.consumer.negative_acknowledge(msg)

    async def _segment_timer(self) -> None:
        """
        Background task to check for devices that need segment generation.

        Handles the case where frames arrive sporadically and we need to
        generate segments based on time threshold rather than frame count.
        """
        while self.running:
            await asyncio.sleep(10)  # Check every 10 seconds

            for state_key, state in list(self.device_states.items()):
                if state.should_generate_segment(
                    self.processing_config.frames_per_segment,
                    max_wait_seconds=self.processing_config.segment_duration_seconds,
                ):
                    lock = self._get_device_lock(state_key)
                    if not lock.locked():
                        async with lock:
                            if state.should_generate_segment(
                                self.processing_config.frames_per_segment,
                                max_wait_seconds=self.processing_config.segment_duration_seconds,
                            ):
                                await self._generate_segment(state)

    async def run(self) -> None:
        """
        Start the consumer and process messages.
        """
        logger.info("=" * 80)
        logger.info("Stream Processor Consumer")
        logger.info("=" * 80)
        logger.info(f"Pulsar URL: {self.config.service_url}")
        logger.info(f"Topic: {self.config.topic}")
        logger.info(f"Subscription: {self.config.subscription}")
        logger.info(f"Max Workers: {self.processing_config.max_workers}")
        logger.info(f"Redis session tracking: {'enabled' if self.session_store else 'disabled'}")
        logger.info(f"Redis playlist store: {'enabled' if self.playlist_store else 'disabled'}")
        logger.info(f"Watermark: {'enabled' if self.watermark_service else 'disabled'}")
        logger.info("=" * 80)

        try:
            # Connect to Redis if session store is configured
            if self.session_store:
                await self.session_store.connect()

            # Connect to Redis if playlist store is configured
            if self.playlist_store:
                await self.playlist_store.connect()

            # Map Python logging level to Pulsar LoggerLevel
            log_level = get_log_level()
            pulsar_log_level = pulsar.LoggerLevel.Error  # Default to Error
            if log_level <= logging.DEBUG:
                pulsar_log_level = pulsar.LoggerLevel.Debug
            elif log_level <= logging.INFO:
                pulsar_log_level = pulsar.LoggerLevel.Info
            elif log_level <= logging.WARNING:
                pulsar_log_level = pulsar.LoggerLevel.Warn

            # Create Pulsar client with configured log level
            self.client = pulsar.Client(
                self.config.service_url,
                logger=pulsar.ConsoleLogger(pulsar_log_level),
            )
            assert self.client is not None  # For type checker

            # Create consumer with Key_Shared subscription
            self.consumer = self.client.subscribe(
                self.config.topic,
                subscription_name=self.config.subscription,
                consumer_type=pulsar.ConsumerType.KeyShared,
                consumer_name=self.config.consumer_name,
                # Process messages one at a time per key (device)
                receiver_queue_size=1000,
            )

            logger.info("Connected to Pulsar broker")
            logger.info("Stream Processor is running. Press Ctrl+C to stop.")

            self.running = True

            # Start segment timer task
            timer_task = asyncio.create_task(self._segment_timer())

            # Main message loop
            while self.running:
                try:
                    # Receive message with timeout
                    assert self.consumer is not None  # For type checker
                    msg = self.consumer.receive(timeout_millis=1000)
                    await self._handle_message(msg)

                except pulsar.Timeout:
                    # No message received, continue
                    continue
                except Exception as e:
                    if self.running:
                        logger.error(f"Error receiving message: {e}")
                        await asyncio.sleep(1)

            # Cancel background tasks
            timer_task.cancel()
            try:
                await timer_task
            except asyncio.CancelledError:
                pass

        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the consumer gracefully."""
        logger.info("Stopping consumer...")
        self.running = False

        # Process any remaining frames
        for state_key, state in self.device_states.items():
            if state.pending_frames:
                logger.info(f"Processing remaining frames for {state_key}")
                try:
                    await self._generate_segment(state)
                except Exception as e:
                    logger.error(f"Error processing remaining frames: {e}")

        # Close Redis session store
        if self.session_store:
            try:
                await self.session_store.close()
            except Exception as e:
                logger.error(f"Error closing Redis session store: {e}")

        # Close Redis playlist store
        if self.playlist_store:
            try:
                await self.playlist_store.close()
            except Exception as e:
                logger.error(f"Error closing Redis playlist store: {e}")

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Close Pulsar resources
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")

        logger.info("Consumer stopped")
