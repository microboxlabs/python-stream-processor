"""Tests for the per-device worker model in StreamProcessorConsumer.

These exercise the decoupled consume/generate path directly (no Pulsar broker):
frames are pushed onto a device's queue and the worker is drained via the same
graceful-shutdown path used in production.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import stream_processor.consumer.pulsar_consumer as pc
from stream_processor.model.events import FrameEvent
from stream_processor.service.redis_playlist_store import RedisPlaylistStore


def _make_event(client_id: str, device_id: str, idx: int) -> FrameEvent:
    """Build a minimal valid FrameEvent."""
    return FrameEvent(
        eventId=f"evt-{device_id}-{idx}",
        clientId=client_id,
        deviceId=device_id,
        timestamp=datetime.now(UTC),
        framePath=f"/frames/{device_id}/{idx}.jpg",
        requestId=f"req-{device_id}-{idx}",
    )


def _make_consumer(monkeypatch, gen_side_effect, highest=-1):
    """Construct a consumer with a mocked HLS generator (no filesystem/FFmpeg)."""
    fake_hls = MagicMock()
    fake_hls.get_highest_segment_number.return_value = highest
    fake_hls.generate_segment.side_effect = gen_side_effect
    # Patch the class so __init__ does not build a real (mkdir-ing) generator.
    monkeypatch.setattr(pc, "HLSGenerator", lambda: fake_hls)
    consumer = pc.StreamProcessorConsumer(use_redis=False)
    consumer.running = True
    return consumer, fake_hls


def _shutdown_executors(consumer) -> None:
    consumer.executor.shutdown(wait=False)
    consumer.io_executor.shutdown(wait=False)


class TestPerDeviceWorker:
    """Core decoupled generation behavior."""

    async def test_generates_one_segment_at_frame_threshold(self, monkeypatch):
        """Pushing frames_per_segment frames triggers exactly one generation."""
        calls: list[tuple] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            calls.append((device_id, list(frames), segment_number))
            return (f"seg_{segment_number:06d}.ts", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)
        try:
            fps = consumer.processing_config.frames_per_segment
            queue = consumer._get_or_create_worker("c:d", "c", "d")
            for i in range(fps):
                await queue.put(_make_event("c", "d", i))

            # Drains the queue, then flushes & exits each worker deterministically.
            await consumer._shutdown_workers()

            assert len(calls) == 1
            device_id, frames, segment_number = calls[0]
            assert device_id == "d"
            assert len(frames) == fps
            assert segment_number == 0  # seeded from highest=-1
        finally:
            _shutdown_executors(consumer)

    async def test_two_devices_number_independently(self, monkeypatch):
        """Each device gets its own worker and its own segment numbering."""
        calls: list[tuple] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            calls.append((device_id, segment_number))
            return (f"seg_{segment_number:06d}.ts", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)
        try:
            fps = consumer.processing_config.frames_per_segment
            qa = consumer._get_or_create_worker("c:a", "c", "a")
            qb = consumer._get_or_create_worker("c:b", "c", "b")
            for i in range(fps):
                await qa.put(_make_event("c", "a", i))
                await qb.put(_make_event("c", "b", i))

            await consumer._shutdown_workers()

            assert len(consumer.device_states) == 2
            by_device = dict(calls)
            assert by_device == {"a": 0, "b": 0}
        finally:
            _shutdown_executors(consumer)

    async def test_partial_frames_flushed_on_shutdown(self, monkeypatch):
        """Frames below threshold are flushed into a segment on graceful stop."""
        calls: list[tuple] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            calls.append((device_id, list(frames), segment_number))
            return (f"seg_{segment_number:06d}.ts", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)
        try:
            queue = consumer._get_or_create_worker("c:d", "c", "d")
            # Three frames: below the default threshold of 6.
            for i in range(3):
                await queue.put(_make_event("c", "d", i))

            await consumer._shutdown_workers()

            assert len(calls) == 1
            assert len(calls[0][1]) == 3
        finally:
            _shutdown_executors(consumer)

    async def test_redis_segment_counter_allocates_number(self, monkeypatch, fake_redis):
        """When enabled, the segment number comes from the atomic Redis counter."""
        calls: list[tuple] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            calls.append((device_id, segment_number))
            return (f"seg_{segment_number:06d}.ts", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen, highest=-1)
        try:
            # Wire a Redis-backed counter and pre-seed it to a distinctive value
            # so the result can only come from Redis (in-memory would be 0).
            store = RedisPlaylistStore()
            store._client = fake_redis
            consumer.playlist_store = store
            consumer.segment_counter_enabled = True
            await fake_redis.set("hls:seq:c:d", 41)  # next INCR -> 42

            fps = consumer.processing_config.frames_per_segment
            queue = consumer._get_or_create_worker("c:d", "c", "d")
            for i in range(fps):
                await queue.put(_make_event("c", "d", i))

            await consumer._shutdown_workers()

            assert len(calls) == 1
            assert calls[0][1] == 42
        finally:
            _shutdown_executors(consumer)
            await fake_redis.flushall()
