"""
Pulsar consumer for frame events with Key_Shared subscription.
Processes frames and triggers HLS segment generation.
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import pulsar

from ..config.settings import settings
from ..model.events import FrameEvent, DeviceState
from ..service.hls_generator import HLSGenerator
from ..utils.logger import get_logger
from ..utils.metrics import (
    frames_received_total,
    active_devices_gauge,
    processing_errors_total,
)

logger = get_logger(__name__)


class StreamProcessorConsumer:
    """
    Pulsar consumer for stream processing.

    Uses Key_Shared subscription to ensure ordering per device
    while allowing horizontal scaling across multiple instances.
    """

    def __init__(self):
        """Initialize the consumer."""
        self.config = settings.pulsar
        self.processing_config = settings.processing
        
        # Device state tracking (in-memory)
        self.device_states: Dict[str, DeviceState] = {}
        
        # HLS generator service
        self.hls_generator = HLSGenerator()
        
        # Thread pool for FFmpeg workers
        self.executor = ThreadPoolExecutor(
            max_workers=self.processing_config.max_workers,
            thread_name_prefix="ffmpeg-worker-"
        )
        
        # Pulsar client and consumer (initialized on run)
        self.client = None
        self.consumer = None
        self.running = False
        
        # Segment generation lock per device
        self.device_locks: Dict[str, asyncio.Lock] = {}

    def _get_or_create_state(self, device_id: str) -> DeviceState:
        """Get or create device state."""
        if device_id not in self.device_states:
            self.device_states[device_id] = DeviceState(device_id=device_id)
            active_devices_gauge.inc()
            logger.info(f"New device registered: {device_id}")
        return self.device_states[device_id]

    def _get_device_lock(self, device_id: str) -> asyncio.Lock:
        """Get or create async lock for device."""
        if device_id not in self.device_locks:
            self.device_locks[device_id] = asyncio.Lock()
        return self.device_locks[device_id]

    async def _process_frame(self, event: FrameEvent) -> None:
        """
        Process a single frame event.
        
        Accumulates frames and triggers segment generation when threshold is reached.
        """
        device_id = event.device_id
        lock = self._get_device_lock(device_id)
        
        async with lock:
            state = self._get_or_create_state(device_id)
            
            # Add frame to pending
            state.add_frame(event.frame_path, event.timestamp)
            frames_received_total.labels(device_id=device_id).inc()
            
            logger.debug(
                f"Frame received for {device_id}: {event.frame_path} "
                f"(pending: {state.frame_count})"
            )
            
            # Check if we should generate a segment
            if state.should_generate_segment(
                self.processing_config.frames_per_segment,
                max_wait_seconds=self.processing_config.segment_duration_seconds
            ):
                await self._generate_segment(state)

    async def _generate_segment(self, state: DeviceState) -> None:
        """Generate HLS segment from accumulated frames."""
        device_id = state.device_id
        frames = state.pending_frames.copy()
        segment_number = state.current_segment_number
        
        if not frames:
            logger.warning(f"No frames to process for {device_id}")
            return
        
        logger.info(
            f"Generating segment {segment_number} for {device_id} "
            f"({len(frames)} frames)"
        )
        
        try:
            # Run FFmpeg in thread pool
            loop = asyncio.get_event_loop()
            segment_path = await loop.run_in_executor(
                self.executor,
                self.hls_generator.generate_segment,
                device_id,
                frames,
                segment_number,
            )
            
            # Clear pending frames after successful generation
            state.clear_pending_frames()
            
            logger.info(f"Segment generated: {segment_path}")
            
        except Exception as e:
            processing_errors_total.labels(device_id=device_id, error_type="ffmpeg").inc()
            logger.error(f"Failed to generate segment for {device_id}: {e}", exc_info=True)

    async def _handle_message(self, msg) -> None:
        """Handle a single Pulsar message."""
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
            
            for device_id, state in list(self.device_states.items()):
                if state.should_generate_segment(
                    self.processing_config.frames_per_segment,
                    max_wait_seconds=self.processing_config.segment_duration_seconds
                ):
                    lock = self._get_device_lock(device_id)
                    if not lock.locked():
                        async with lock:
                            if state.should_generate_segment(
                                self.processing_config.frames_per_segment,
                                max_wait_seconds=self.processing_config.segment_duration_seconds
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
        logger.info("=" * 80)
        
        try:
            # Create Pulsar client
            self.client = pulsar.Client(self.config.service_url)
            
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
                    msg = self.consumer.receive(timeout_millis=1000)
                    await self._handle_message(msg)
                    
                except pulsar.Timeout:
                    # No message received, continue
                    continue
                except Exception as e:
                    if self.running:
                        logger.error(f"Error receiving message: {e}")
                        await asyncio.sleep(1)
            
            # Cancel timer task
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
        for device_id, state in self.device_states.items():
            if state.pending_frames:
                logger.info(f"Processing remaining frames for {device_id}")
                try:
                    await self._generate_segment(state)
                except Exception as e:
                    logger.error(f"Error processing remaining frames: {e}")
        
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

