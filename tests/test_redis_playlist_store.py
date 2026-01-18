"""Tests for RedisPlaylistStore service."""

import time

import pytest


class TestRedisPlaylistStoreAddSegment:
    """Tests for add_segment method."""

    async def test_add_segment_returns_true_for_new_segment(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that add_segment returns True when adding a new segment."""
        result = await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )

        assert result is True

    async def test_add_segment_returns_false_for_duplicate(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that add_segment returns False when segment already exists."""
        # Add segment first time
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )

        # Try to add same segment again (updates score, returns False)
        result = await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574500.0
        )

        assert result is False

    async def test_add_segment_uses_current_time_if_not_provided(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that add_segment uses current time when timestamp is not provided."""
        before = time.time()
        await playlist_store.add_segment(sample_client_id, sample_device_id, segment_number=100)
        after = time.time()

        # Get the segment to verify timestamp
        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=0, to_timestamp=after + 1
        )

        assert len(segments) == 1
        segment_num, timestamp = segments[0]
        assert segment_num == 100
        assert before <= timestamp <= after

    async def test_add_multiple_segments(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test adding multiple segments."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=1705574430.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=1705574460.0
        )

        count = await playlist_store.get_segment_count(sample_client_id, sample_device_id)
        assert count == 3


class TestRedisPlaylistStoreRemoveSegment:
    """Tests for remove_segment method."""

    async def test_remove_segment_returns_true_when_exists(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that remove_segment returns True when segment exists."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )

        result = await playlist_store.remove_segment(
            sample_client_id, sample_device_id, segment_number=100
        )

        assert result is True

    async def test_remove_segment_returns_false_when_not_exists(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that remove_segment returns False when segment doesn't exist."""
        result = await playlist_store.remove_segment(
            sample_client_id, sample_device_id, segment_number=999
        )

        assert result is False

    async def test_remove_segment_decreases_count(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that removing a segment decreases the count."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=1705574430.0
        )

        assert await playlist_store.get_segment_count(sample_client_id, sample_device_id) == 2

        await playlist_store.remove_segment(sample_client_id, sample_device_id, segment_number=100)

        assert await playlist_store.get_segment_count(sample_client_id, sample_device_id) == 1


class TestRedisPlaylistStoreRemoveSegmentsBefore:
    """Tests for remove_segments_before method."""

    async def test_remove_segments_before_removes_old_only(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that only segments older than cutoff are removed."""
        # Add segments with different timestamps
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0  # Old
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0  # Old
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=3000.0  # New
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=103, timestamp=4000.0  # New
        )

        # Remove segments before timestamp 2500
        removed = await playlist_store.remove_segments_before(
            sample_client_id, sample_device_id, cutoff_timestamp=2500.0
        )

        assert removed == 2
        assert await playlist_store.get_segment_count(sample_client_id, sample_device_id) == 2

        # Verify remaining segments
        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=0, to_timestamp=10000
        )
        segment_numbers = [s[0] for s in segments]
        assert 100 not in segment_numbers
        assert 101 not in segment_numbers
        assert 102 in segment_numbers
        assert 103 in segment_numbers

    async def test_remove_segments_before_returns_zero_when_all_recent(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that zero is returned when all segments are recent."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=5000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=6000.0
        )

        # Try to remove segments before timestamp 1000 (none qualify)
        removed = await playlist_store.remove_segments_before(
            sample_client_id, sample_device_id, cutoff_timestamp=1000.0
        )

        assert removed == 0
        assert await playlist_store.get_segment_count(sample_client_id, sample_device_id) == 2

    async def test_remove_segments_before_removes_all_when_all_old(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that all segments are removed when all are old."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )

        # Remove all segments (cutoff far in the future)
        removed = await playlist_store.remove_segments_before(
            sample_client_id, sample_device_id, cutoff_timestamp=9999999.0
        )

        assert removed == 2
        assert await playlist_store.get_segment_count(sample_client_id, sample_device_id) == 0

    async def test_remove_segments_before_handles_empty_store(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that remove_segments_before works on empty store."""
        removed = await playlist_store.remove_segments_before(
            sample_client_id, sample_device_id, cutoff_timestamp=9999999.0
        )

        assert removed == 0


class TestRedisPlaylistStoreGetSegments:
    """Tests for get_segments method."""

    async def test_get_segments_returns_segments_in_range(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that get_segments returns segments within the time range."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=3000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=103, timestamp=4000.0
        )

        # Get segments in range 1500-3500
        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=1500.0, to_timestamp=3500.0
        )

        assert len(segments) == 2
        segment_numbers = [s[0] for s in segments]
        assert 101 in segment_numbers
        assert 102 in segment_numbers

    async def test_get_segments_returns_empty_when_none_in_range(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that get_segments returns empty list when no segments in range."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )

        # Query range with no segments
        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=5000.0, to_timestamp=6000.0
        )

        assert len(segments) == 0

    async def test_get_segments_returns_empty_for_nonexistent_device(
        self, playlist_store, sample_client_id
    ):
        """Test that get_segments returns empty list for nonexistent device."""
        segments = await playlist_store.get_segments(
            sample_client_id, "nonexistent-device", from_timestamp=0, to_timestamp=9999999
        )

        assert len(segments) == 0

    async def test_get_segments_ordered_by_timestamp(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that segments are returned ordered by timestamp."""
        # Add segments in non-sequential order
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=3000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )

        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=0, to_timestamp=9999999
        )

        # Should be ordered by timestamp
        assert segments[0][0] == 100
        assert segments[1][0] == 101
        assert segments[2][0] == 102

    async def test_get_segments_returns_tuples_with_timestamp(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that get_segments returns tuples of (segment_number, timestamp)."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1705574400.0
        )

        segments = await playlist_store.get_segments(
            sample_client_id, sample_device_id, from_timestamp=0, to_timestamp=9999999999
        )

        assert len(segments) == 1
        segment_num, timestamp = segments[0]
        assert segment_num == 100
        assert timestamp == 1705574400.0


class TestRedisPlaylistStoreGetSegmentCount:
    """Tests for get_segment_count method."""

    async def test_get_segment_count_returns_correct_count(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that get_segment_count returns the correct count."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=3000.0
        )

        count = await playlist_store.get_segment_count(sample_client_id, sample_device_id)

        assert count == 3

    async def test_get_segment_count_returns_zero_for_empty(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that get_segment_count returns 0 for empty store."""
        count = await playlist_store.get_segment_count(sample_client_id, sample_device_id)

        assert count == 0

    async def test_get_segment_count_returns_zero_for_nonexistent_device(
        self, playlist_store, sample_client_id
    ):
        """Test that get_segment_count returns 0 for nonexistent device."""
        count = await playlist_store.get_segment_count(sample_client_id, "nonexistent-device")

        assert count == 0


class TestRedisPlaylistStoreDeletePlaylist:
    """Tests for delete_playlist method."""

    async def test_delete_playlist_returns_true_when_exists(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that delete_playlist returns True when playlist exists."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )

        result = await playlist_store.delete_playlist(sample_client_id, sample_device_id)

        assert result is True

    async def test_delete_playlist_returns_false_when_not_exists(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that delete_playlist returns False when playlist doesn't exist."""
        result = await playlist_store.delete_playlist(sample_client_id, sample_device_id)

        assert result is False

    async def test_delete_playlist_removes_all_segments(
        self, playlist_store, sample_client_id, sample_device_id
    ):
        """Test that delete_playlist removes all segments for the device."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=102, timestamp=3000.0
        )

        await playlist_store.delete_playlist(sample_client_id, sample_device_id)

        count = await playlist_store.get_segment_count(sample_client_id, sample_device_id)
        assert count == 0


class TestRedisPlaylistStoreMultipleDevices:
    """Tests for multiple devices/clients."""

    async def test_segments_isolated_between_devices(
        self, playlist_store, sample_client_id
    ):
        """Test that segments are isolated between different devices."""
        device1 = "device-001"
        device2 = "device-002"

        # Add segments to device1
        await playlist_store.add_segment(
            sample_client_id, device1, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, device1, segment_number=101, timestamp=2000.0
        )

        # Add segments to device2
        await playlist_store.add_segment(
            sample_client_id, device2, segment_number=200, timestamp=1000.0
        )

        # Verify counts are separate
        assert await playlist_store.get_segment_count(sample_client_id, device1) == 2
        assert await playlist_store.get_segment_count(sample_client_id, device2) == 1

        # Verify segment data is separate
        segments1 = await playlist_store.get_segments(
            sample_client_id, device1, from_timestamp=0, to_timestamp=9999999
        )
        segments2 = await playlist_store.get_segments(
            sample_client_id, device2, from_timestamp=0, to_timestamp=9999999
        )

        assert len(segments1) == 2
        assert len(segments2) == 1
        assert segments1[0][0] == 100
        assert segments2[0][0] == 200

    async def test_segments_isolated_between_clients(
        self, playlist_store, sample_device_id
    ):
        """Test that segments are isolated between different clients."""
        client1 = "client-001"
        client2 = "client-002"

        # Add segments to client1
        await playlist_store.add_segment(
            client1, sample_device_id, segment_number=100, timestamp=1000.0
        )

        # Add segments to client2
        await playlist_store.add_segment(
            client2, sample_device_id, segment_number=200, timestamp=1000.0
        )

        # Verify counts are separate
        assert await playlist_store.get_segment_count(client1, sample_device_id) == 1
        assert await playlist_store.get_segment_count(client2, sample_device_id) == 1

    async def test_delete_playlist_only_affects_target_device(
        self, playlist_store, sample_client_id
    ):
        """Test that delete_playlist only affects the target device."""
        device1 = "device-001"
        device2 = "device-002"

        # Add segments to both devices
        await playlist_store.add_segment(
            sample_client_id, device1, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, device2, segment_number=200, timestamp=1000.0
        )

        # Delete only device1's playlist
        await playlist_store.delete_playlist(sample_client_id, device1)

        # Verify device1 is empty but device2 is intact
        assert await playlist_store.get_segment_count(sample_client_id, device1) == 0
        assert await playlist_store.get_segment_count(sample_client_id, device2) == 1


class TestRedisPlaylistStoreKeyFormat:
    """Tests for Redis key format."""

    async def test_segments_key_format(
        self, playlist_store, fake_redis, sample_client_id, sample_device_id
    ):
        """Test that the Redis key format is correct."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )

        # Check that the key exists with the expected format
        expected_key = f"hls:segments:{sample_client_id}:{sample_device_id}"
        exists = await fake_redis.exists(expected_key)

        assert exists == 1

    async def test_segment_stored_as_zset(
        self, playlist_store, fake_redis, sample_client_id, sample_device_id
    ):
        """Test that segments are stored as a sorted set (ZSET)."""
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=100, timestamp=1000.0
        )
        await playlist_store.add_segment(
            sample_client_id, sample_device_id, segment_number=101, timestamp=2000.0
        )

        key = f"hls:segments:{sample_client_id}:{sample_device_id}"

        # Verify it's a ZSET by using ZRANGE
        members = await fake_redis.zrange(key, 0, -1, withscores=True)

        assert len(members) == 2
        # Members should be (member, score) tuples
        assert ("100", 1000.0) in members
        assert ("101", 2000.0) in members
