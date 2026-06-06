"""Tests for the per-device worker model in StreamProcessorConsumer.

These exercise the decoupled consume/generate path directly (no Pulsar broker):
frames are pushed onto a device's queue and the worker is drained via the same
graceful-shutdown path used in production.
"""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pulsar

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


def _pulsar_msg(client_id: str, device_id: str, idx: int):
    """A fake Pulsar message whose .data() is a valid FrameEvent JSON."""
    m = MagicMock()
    m.data.return_value = json.dumps(
        {
            "eventId": f"e-{device_id}-{idx}",
            "clientId": client_id,
            "deviceId": device_id,
            "timestamp": "2025-11-25T10:30:00Z",
            "framePath": f"/frames/{device_id}/{idx}.jpg",
            "requestId": f"r-{device_id}-{idx}",
        }
    ).encode()
    return m


class TestBatchReceive:
    """The batched Pulsar intake path."""

    def test_blocking_batch_receive_returns_list(self, monkeypatch):
        """batch_receive results are returned as a plain list."""
        consumer, _ = _make_consumer(monkeypatch, lambda *a: ("u", 1.0))
        try:
            m1, m2 = object(), object()
            fake = MagicMock()
            fake.batch_receive.return_value = [m1, m2]
            consumer.consumer = fake
            assert consumer._blocking_batch_receive() == [m1, m2]
        finally:
            _shutdown_executors(consumer)

    def test_blocking_batch_receive_timeout_returns_empty(self, monkeypatch):
        """A batch timeout yields an empty list, not an exception."""
        consumer, _ = _make_consumer(monkeypatch, lambda *a: ("u", 1.0))
        try:
            fake = MagicMock()
            fake.batch_receive.side_effect = pulsar.Timeout
            consumer.consumer = fake
            assert consumer._blocking_batch_receive() == []
        finally:
            _shutdown_executors(consumer)

    async def test_dispatch_batch_routes_and_acks(self, monkeypatch):
        """Each message in a batch is routed to its device worker and acked."""
        calls: list[tuple] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            calls.append((device_id, segment_number))
            return (f"seg_{segment_number:06d}.ts", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)
        consumer.consumer = MagicMock()  # for acknowledge()
        try:
            fps = consumer.processing_config.frames_per_segment
            # Interleave two devices, fps frames each — mimics a batch.
            for i in range(fps):
                await consumer._dispatch(_pulsar_msg("c", "a", i))
                await consumer._dispatch(_pulsar_msg("c", "b", i))

            await consumer._shutdown_workers()

            assert consumer.consumer.acknowledge.call_count == fps * 2
            assert {d for d, _ in calls} == {"a", "b"}
        finally:
            _shutdown_executors(consumer)


class TestParallelWatermark:
    """Watermarking is deferred out of accumulation and run in parallel per segment."""

    async def test_generate_segment_watermarks_in_parallel_and_cleans(self, monkeypatch, tmp_path):
        from unittest.mock import AsyncMock

        wm_root = str(tmp_path / "wm")
        gen_frames: list[list[str]] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            gen_frames.append(list(frames))
            return ("uri", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)

        wm = MagicMock()
        wm_calls: list[tuple] = []

        def _wm(path, req):
            wm_calls.append((path, req))
            return f"{wm_root}/wm_{path.rsplit('/', 1)[-1]}"

        wm.add_timestamp_watermark = AsyncMock(side_effect=_wm)
        wm.is_watermark_temp.side_effect = lambda p: p.startswith(wm_root)
        cleaned: list[str] = []
        wm.cleanup_temp_frames.side_effect = cleaned.extend
        consumer.watermark_service = wm

        fps = consumer.processing_config.frames_per_segment
        queue = consumer._get_or_create_worker("c:d", "c", "d")
        for i in range(fps):
            ev = FrameEvent(
                eventId=f"e{i}",
                clientId="c",
                deviceId="d",
                timestamp=datetime.now(UTC),
                requestTimestamp=1_700_000_000 + i,
                framePath=f"/frames/d/{i}.jpg",
                requestId=f"r{i}",
            )
            await queue.put(ev)

        await consumer._shutdown_workers()

        # Exactly one segment, encoded from the WATERMARKED paths.
        assert len(gen_frames) == 1
        assert all(p.startswith(wm_root) for p in gen_frames[0])
        # One watermark call per frame (run concurrently via gather).
        assert len(wm_calls) == fps
        # Temps handed to cleanup after the segment.
        assert len(cleaned) == fps
        _shutdown_executors(consumer)

    async def test_frames_without_request_time_are_not_watermarked(self, monkeypatch):
        from unittest.mock import AsyncMock

        gen_frames: list[list[str]] = []

        def gen(client_id, device_id, frames, segment_number, offset):
            gen_frames.append(list(frames))
            return ("uri", float(len(frames)))

        consumer, _ = _make_consumer(monkeypatch, gen)
        wm = MagicMock()
        wm.add_timestamp_watermark = AsyncMock(side_effect=AssertionError("should not watermark"))
        wm.is_watermark_temp.side_effect = lambda p: False
        wm.cleanup_temp_frames.side_effect = lambda paths: None
        consumer.watermark_service = wm

        fps = consumer.processing_config.frames_per_segment
        queue = consumer._get_or_create_worker("c:d", "c", "d")
        for i in range(fps):
            # No requestTimestamp -> no watermark for that frame.
            await queue.put(_make_event("c", "d", i))

        await consumer._shutdown_workers()

        assert len(gen_frames) == 1
        # Raw frame paths passed straight through (no watermark applied).
        assert gen_frames[0] == [f"/frames/d/{i}.jpg" for i in range(fps)]
        wm.add_timestamp_watermark.assert_not_called()
        _shutdown_executors(consumer)
