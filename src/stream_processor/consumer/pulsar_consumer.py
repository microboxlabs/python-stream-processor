"""
Pulsar consumer for frame events with Key_Shared subscription.

Consumption is decoupled from segment generation: the receive loop only
parses, routes each frame to a per-device queue, and acks. One asyncio worker
per device drains its queue and submits FFmpeg jobs to a thread pool. Because
each device has its own worker, segments for different devices are generated
concurrently (bounded by the worker pool), while ordering within a device is
preserved.
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
    queued_frames_gauge,
)

logger = get_logger(__name__)


class StreamProcessorConsumer:
    """
    Pulsar consumer for stream processing.

    Uses Key_Shared subscription to ensure ordering per device while allowing
    horizontal scaling across multiple instances. Within a single instance,
    a per-device worker model provides concurrency across devices.
    """

    # Sentinel pushed onto a device queue to tell its worker to drain and exit.
    _SHUTDOWN = object()

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

        # Device state tracking (in-memory). Each device's state is owned
        # exclusively by its worker coroutine, so no per-device lock is needed.
        self.device_states: dict[str, DeviceState] = {}

        # Per-device frame queues and the worker task draining each one.
        self.device_queues: dict[str, asyncio.Queue] = {}
        self.device_workers: dict[str, asyncio.Task] = {}

        # HLS generator service
        self.hls_generator = HLSGenerator()

        # Watermark service (only initialized if enabled)
        self.watermark_service: WatermarkService | None = None
        if self.watermark_config.enabled:
            self.watermark_service = WatermarkService(self.watermark_config)
            logger.info("Watermark service enabled")

        # Thread pool for FFmpeg workers (bounds concurrent segment generation).
        self.executor = ThreadPoolExecutor(
            max_workers=self.processing_config.max_workers, thread_name_prefix="ffmpeg-worker-"
        )

        # Dedicated single thread for the blocking Pulsar receive() call so it
        # never stalls the asyncio event loop.
        self.io_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pulsar-recv-")

        # Pulsar client and consumer (initialized on run)
        self.client: pulsar.Client | None = None
        self.consumer: pulsar.Consumer | None = None
        self.running = False
        self._stopping = False

        # Redis session store for distributed offline detection
        # The offline-checker service reads this to detect offline devices
        self.session_store: RedisSessionStore | None = None

        if use_redis and settings.redis.enabled and self.archive_config.enabled:
            self.session_store = RedisSessionStore()
            logger.info("Redis session tracking enabled for offline detection")

        # Redis playlist store: hosts both the on-the-fly playlist metadata and
        # the atomic per-device segment counter. Either feature needs the store.
        self.playlist_enabled = (
            use_redis and settings.redis.enabled and settings.redis.playlist_enabled
        )
        self.segment_counter_enabled = (
            use_redis and settings.redis.enabled and settings.redis.segment_counter_enabled
        )
        self.playlist_store: RedisPlaylistStore | None = None
        if self.playlist_enabled or self.segment_counter_enabled:
            self.playlist_store = RedisPlaylistStore()
            logger.info(
                "Redis playlist store enabled "
                f"(playlist={self.playlist_enabled}, segment_counter={self.segment_counter_enabled})"
            )

    async def _init_device_state(self, client_id: str, device_id: str) -> DeviceState:
        """
        Create and register state for a device on first sight.

        The storage scan that resumes segment numbering is offloaded to the
        thread pool so it never blocks the event loop, and (when enabled) the
        Redis segment counter is seeded so multi-pod numbering stays atomic.
        """
        state_key = f"{client_id}:{device_id}"

        loop = asyncio.get_event_loop()
        highest = await loop.run_in_executor(
            self.executor,
            self.hls_generator.get_highest_segment_number,
            client_id,
            device_id,
        )
        initial_segment = highest + 1 if highest >= 0 else 0

        state = DeviceState(
            client_id=client_id,
            device_id=device_id,
            current_segment_number=initial_segment,
        )
        self.device_states[state_key] = state
        active_devices_gauge.inc()

        if self.segment_counter_enabled and self.playlist_store:
            try:
                await self.playlist_store.seed_segment_counter(
                    client_id, device_id, initial_segment
                )
            except Exception as e:
                logger.warning(f"Failed to seed Redis segment counter for {state_key}: {e}")

        logger.info(f"New device registered: {state_key} (starting at segment {initial_segment})")
        return state

    def _get_or_create_worker(
        self, state_key: str, client_id: str, device_id: str
    ) -> asyncio.Queue:
        """Get the device's frame queue, spawning its worker task on first sight."""
        queue = self.device_queues.get(state_key)
        if queue is None:
            queue = asyncio.Queue(maxsize=self.processing_config.device_queue_maxsize)
            self.device_queues[state_key] = queue
            self.device_workers[state_key] = asyncio.create_task(
                self._device_worker(state_key, client_id, device_id, queue)
            )
        return queue

    async def _device_worker(
        self, state_key: str, client_id: str, device_id: str, queue: asyncio.Queue
    ) -> None:
        """
        Drain one device's frame queue and generate its segments serially.

        Blocking on `_generate_segment` here only serializes *this* device;
        other device workers keep running on the event loop, so generation is
        concurrent across devices (bounded by the FFmpeg thread pool).
        """
        try:
            state = await self._init_device_state(client_id, device_id)
        except Exception as e:
            logger.error(f"Failed to initialize device state for {state_key}: {e}", exc_info=True)
            # Unregister so a later frame can retry creating the worker.
            self.device_queues.pop(state_key, None)
            self.device_workers.pop(state_key, None)
            return

        frames_per_segment = self.processing_config.frames_per_segment
        max_wait = self.processing_config.max_segment_wait_seconds
        interval = self.processing_config.segment_timer_interval_seconds

        try:
            shutting_down = False
            while not shutting_down:
                shutting_down = await self._consume_one(state, queue, interval)
                if state.should_generate_segment(frames_per_segment, max_wait_seconds=max_wait):
                    await self._generate_segment(state)
        finally:
            # Flush remaining frames so a graceful stop doesn't drop a partial segment.
            await self._flush_pending_frames(state)

    async def _consume_one(self, state: DeviceState, queue: asyncio.Queue, interval: int) -> bool:
        """
        Pull one queued item (or time out) and fold it into the device state.

        Waking on the timeout lets time-based segments flush when frames arrive
        sporadically. Returns True when the shutdown sentinel is received.
        """
        try:
            item = await asyncio.wait_for(queue.get(), timeout=interval)
        except TimeoutError:
            return False

        if item is self._SHUTDOWN:
            return True

        if isinstance(item, FrameEvent):
            try:
                await self._accumulate_frame(state, item)
            except Exception as e:
                processing_errors_total.labels(
                    device_id=state.state_key, error_type="accumulate"
                ).inc()
                logger.error(f"Error accumulating frame for {state.state_key}: {e}", exc_info=True)
        return False

    async def _flush_pending_frames(self, state: DeviceState) -> None:
        """Generate a final segment from any leftover frames (used on shutdown)."""
        if not state.pending_frames:
            return
        try:
            await self._generate_segment(state)
        except Exception as e:
            logger.error(f"Error flushing frames for {state.state_key} on shutdown: {e}")

    async def _accumulate_frame(self, state: DeviceState, event: FrameEvent) -> None:
        """Apply optional watermark, append the frame, and update session activity."""
        # Apply watermark if enabled
        frame_path = event.frame_path
        if self.watermark_service and event.request_timestamp:
            try:
                frame_path = await self.watermark_service.add_timestamp_watermark(
                    event.frame_path, event.request_timestamp
                )
                logger.debug(f"Watermark applied to {frame_path}")
            except Exception as e:
                logger.error(f"Failed to apply watermark to {event.frame_path}: {e}")
                # Continue with original frame path if watermarking fails
                frame_path = event.frame_path

        state.add_frame(frame_path, event.timestamp)
        frames_received_total.labels(device_id=state.state_key).inc()

        # Update session activity in Redis (for offline-checker service).
        # The returned SessionData lets us detect a session boundary and reset
        # the per-session cumulative PTS offset so each archive starts at PTS=0
        # and stays continuous within the session.
        if self.session_store:
            session_data = await self.session_store.update_activity(
                state.client_id, state.device_id
            )
            if session_data is not None and session_data.session_id != state.current_session_id:
                if state.current_session_id is not None:
                    logger.info(
                        f"Session boundary for {state.state_key}: "
                        f"{state.current_session_id} -> {session_data.session_id}; "
                        f"resetting PTS offset"
                    )
                state.reset_for_session(session_data.session_id)

        logger.debug(
            f"Frame received for {state.state_key}: {frame_path} (pending: {state.frame_count})"
        )

    async def _next_segment_number(self, state: DeviceState) -> int:
        """
        Allocate the next segment number for a device.

        Prefers the atomic Redis counter (safe across pods); falls back to the
        in-memory counter if Redis is disabled or momentarily unavailable.
        """
        if self.segment_counter_enabled and self.playlist_store:
            try:
                return await self.playlist_store.next_segment_number(
                    state.client_id, state.device_id
                )
            except Exception as e:
                logger.warning(
                    f"Redis segment counter unavailable for {state.state_key}, "
                    f"falling back to in-memory numbering: {e}"
                )
        return state.current_segment_number

    async def _add_segment_to_playlist(
        self,
        client_id: str,
        device_id: str,
        segment_number: int,
        timestamp: float | None = None,
    ) -> None:
        """Add segment to playlist store in Redis for dynamic playlist generation.

        ``timestamp`` is the segment's first-frame capture time (epoch seconds),
        used as the ZSET score so the playlist orders by content time — correct
        even if segments are generated out of order (parallel catch-up). The
        quarkus playlist reads this ordering and the cleanup service uses it for
        time-based retention, so a content timestamp satisfies both. Falls back
        to "now" when unavailable.
        """
        if not (self.playlist_store and self.playlist_enabled):
            return
        try:
            await self.playlist_store.add_segment(
                client_id, device_id, segment_number, timestamp=timestamp
            )
        except Exception as e:
            logger.warning(
                f"Failed to add segment to playlist store "
                f"(client={client_id}, device={device_id}, segment={segment_number}): {e}"
            )

    async def _generate_segment(self, state: DeviceState) -> None:
        """Generate HLS segment from accumulated frames."""
        client_id = state.client_id
        device_id = state.device_id
        state_key = state.state_key
        frames = state.pending_frames.copy()

        if not frames:
            logger.warning(f"No frames to process for {state_key}")
            return

        # Capture the segment's content timestamp before clear_pending_frames()
        # resets it; used as the playlist-store score (content-time ordering).
        segment_score = (
            state.pending_first_frame_time.timestamp()
            if state.pending_first_frame_time is not None
            else None
        )

        segment_number = await self._next_segment_number(state)

        logger.info(f"Generating segment {segment_number} for {state_key} ({len(frames)} frames)")

        try:
            # Run FFmpeg in thread pool. Pass the per-session cumulative offset
            # so the generated segment's PTS continues the timeline.
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self.hls_generator.generate_segment,
                client_id,
                device_id,
                frames,
                segment_number,
                state.cumulative_segment_seconds,
            )

            # Clear pending frames (even if generation was skipped)
            state.clear_pending_frames()

            if result:
                final_uri, media_duration = result
                state.advance_cumulative(media_duration)
                logger.info(
                    f"Segment generated: {final_uri} "
                    f"(media={media_duration:.3f}s, "
                    f"cumulative={state.cumulative_segment_seconds:.3f}s)"
                )

                # Update session segment info in Redis (for offline-checker service)
                if self.session_store:
                    await self.session_store.update_segment(client_id, device_id, segment_number)

                await self._add_segment_to_playlist(
                    client_id, device_id, segment_number, segment_score
                )
            else:
                logger.debug(f"Segment generation skipped for {state_key} (missing frames)")

        except Exception as e:
            processing_errors_total.labels(device_id=state_key, error_type="ffmpeg").inc()
            logger.error(f"Failed to generate segment for {state_key}: {e}", exc_info=True)
        finally:
            # Remove watermarked temp frames now that FFmpeg has consumed them.
            if self.watermark_service:
                self.watermark_service.cleanup_temp_frames(frames)

    def _blocking_batch_receive(self) -> list[pulsar.Message]:
        """
        Blocking batch receive, run on the dedicated io thread.

        Returns up to receive_batch_size messages (fewer if the batch timeout
        elapses first), or an empty list on timeout. Batching keeps consumer
        intake from being capped at one receive round-trip per message.
        """
        assert self.consumer is not None
        try:
            return list(self.consumer.batch_receive())
        except pulsar.Timeout:
            return []

    async def _dispatch(self, msg: pulsar.Message) -> None:
        """Parse a Pulsar message, route the frame to its device worker, and ack."""
        if self.consumer is None:
            logger.error("Consumer not initialized")
            return

        try:
            data = json.loads(msg.data().decode("utf-8"))
            event = FrameEvent.model_validate(data)
        except json.JSONDecodeError as e:
            processing_errors_total.labels(device_id="unknown", error_type="json_parse").inc()
            logger.error(f"Failed to parse message: {e}")
            self.consumer.negative_acknowledge(msg)
            return
        except Exception as e:
            processing_errors_total.labels(device_id="unknown", error_type="processing").inc()
            logger.error(f"Error parsing message: {e}", exc_info=True)
            self.consumer.negative_acknowledge(msg)
            return

        client_id = event.client_id
        device_id = sanitize_path_component(event.device_id)
        state_key = f"{client_id}:{device_id}"
        queue = self._get_or_create_worker(state_key, client_id, device_id)

        # Backpressure: when this device's queue is full, awaiting put parks
        # only this receive coroutine — device workers keep draining on the
        # event loop — so the broker slows delivery instead of us buffering
        # frames unbounded in memory.
        await queue.put(event)
        self.consumer.acknowledge(msg)

    async def _update_metrics_loop(self) -> None:
        """Periodically publish the total queued-frame backlog for observability."""
        while self.running:
            await asyncio.sleep(5)
            try:
                total = sum(q.qsize() for q in self.device_queues.values())
                queued_frames_gauge.set(total)
            except Exception:
                pass

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
        logger.info(f"Device queue maxsize: {self.processing_config.device_queue_maxsize}")
        logger.info(
            f"Receive batch: size={self.processing_config.receive_batch_size} "
            f"timeout_ms={self.processing_config.receive_batch_timeout_ms}"
        )
        logger.info(f"Redis session tracking: {'enabled' if self.session_store else 'disabled'}")
        logger.info(f"Redis playlist store: {'enabled' if self.playlist_enabled else 'disabled'}")
        logger.info(
            f"Redis segment counter: {'enabled' if self.segment_counter_enabled else 'disabled'}"
        )
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

            # Pull messages in batches so intake isn't capped by one receive
            # round-trip per message. The batch completes as soon as ANY of:
            # batch_size messages, ~10MB, or batch_timeout_ms elapsed.
            batch_policy = pulsar.ConsumerBatchReceivePolicy(
                self.processing_config.receive_batch_size,
                10 * 1024 * 1024,
                self.processing_config.receive_batch_timeout_ms,
            )

            # Create consumer with Key_Shared subscription
            self.consumer = self.client.subscribe(
                self.config.topic,
                subscription_name=self.config.subscription,
                consumer_type=pulsar.ConsumerType.KeyShared,
                consumer_name=self.config.consumer_name,
                receiver_queue_size=max(1000, self.processing_config.receive_batch_size * 2),
                batch_receive_policy=batch_policy,
            )

            logger.info("Connected to Pulsar broker")
            logger.info("Stream Processor is running. Press Ctrl+C to stop.")

            self.running = True

            # Start background metrics task
            metrics_task = asyncio.create_task(self._update_metrics_loop())

            # Main receive loop: batch-receive (off the event loop) -> dispatch
            # each in order -> ack. Segment generation happens in per-device
            # workers, not here.
            loop = asyncio.get_event_loop()
            while self.running:
                try:
                    msgs = await loop.run_in_executor(
                        self.io_executor, self._blocking_batch_receive
                    )
                    for msg in msgs:
                        # Stop mid-batch on shutdown so we don't re-create workers
                        # or block on a full queue after stop() has run. Undispatched
                        # messages are unacked and redelivered by Pulsar.
                        if not self.running:
                            break
                        await self._dispatch(msg)
                except Exception as e:
                    if self.running:
                        logger.error(f"Error receiving messages: {e}")
                        await asyncio.sleep(1)

            # Cancel background tasks
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass

        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def _shutdown_workers(self) -> None:
        """Signal all device workers to flush and exit, then await them."""
        if not self.device_workers:
            return

        logger.info(f"Draining {len(self.device_workers)} device worker(s)...")

        # Snapshot before awaiting: a worker may still be created/removed on the
        # event loop while we await the puts below.
        queues = list(self.device_queues.values())
        workers = list(self.device_workers.items())

        # Workers keep draining the event loop, so these puts succeed even on
        # bounded queues. The sentinel is the last item enqueued (the receive
        # loop has already stopped), so all real frames are processed first.
        for queue in queues:
            await queue.put(self._SHUTDOWN)

        results = await asyncio.gather(*(task for _, task in workers), return_exceptions=True)
        for (state_key, _), result in zip(workers, results, strict=False):
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                logger.error(f"Device worker {state_key} ended with error: {result}")

        self.device_workers.clear()
        self.device_queues.clear()
        logger.info("All device workers stopped")

    async def _close_async_resource(self, resource, name: str) -> None:
        """Safely close an async resource with error logging."""
        if not resource:
            return
        try:
            await resource.close()
        except Exception as e:
            logger.error(f"Error closing {name}: {e}")

    def _close_sync_resource(self, resource, name: str) -> None:
        """Safely close a sync resource with error logging."""
        if not resource:
            return
        try:
            resource.close()
        except Exception as e:
            logger.error(f"Error closing {name}: {e}")

    async def stop(self) -> None:
        """Stop the consumer gracefully."""
        if self._stopping:
            return
        self._stopping = True

        logger.info("Stopping consumer...")
        self.running = False

        await self._shutdown_workers()
        await self._close_async_resource(self.session_store, "Redis session store")
        await self._close_async_resource(self.playlist_store, "Redis playlist store")

        self.executor.shutdown(wait=True)
        self.io_executor.shutdown(wait=True)

        self._close_sync_resource(self.consumer, "consumer")
        self._close_sync_resource(self.client, "client")

        logger.info("Consumer stopped")
