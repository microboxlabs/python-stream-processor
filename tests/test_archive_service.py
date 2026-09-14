"""Tests for expired-archive reaping."""

import pytest

from stream_processor.service.archive_service import ArchiveService

from .test_cleanup_service import CountingStorage


class FakePool:
    """Records the SQL the service runs and replays canned rows."""

    def __init__(self, rows):
        self.rows = rows
        self.fetched: list[str] = []
        self.executed: list[tuple[str, tuple]] = []

    async def fetch(self, query, *args):
        self.fetched.append(query)
        return self.rows

    async def execute(self, query, *args):
        self.executed.append((query, args))


@pytest.fixture
def service_and_pool():
    rows = [
        {
            "id": 1,
            "client_id": "client-a",
            "device_id": "device-1",
            "session_id": "sess-ready",
            "archive_path": "archives/sess-ready",
        },
        {
            "id": 2,
            "client_id": "client-a",
            "device_id": "device-1",
            "session_id": "sess-failed",
            "archive_path": "archives/sess-failed",
        },
    ]
    storage = CountingStorage()
    service = ArchiveService(storage=storage)
    pool = FakePool(rows)
    service._db_pool = pool
    return service, pool, storage


class TestCleanupExpiredArchives:
    async def test_selects_failed_archives_too(self, service_and_pool):
        """'failed' rows keep partially written segments; skipping them leaked storage."""
        service, pool, _ = service_and_pool

        await service.cleanup_expired_archives()

        query = " ".join(pool.fetched[0].split())
        assert "status IN ('ready', 'failed')" in query
        assert "expires_at < CURRENT_TIMESTAMP" in query

    async def test_deletes_storage_for_every_expired_row(self, service_and_pool):
        service, _, storage = service_and_pool

        deleted = await service.cleanup_expired_archives()

        assert deleted == 2
        assert ("client-a", "device-1", "archives/sess-failed/playlist.m3u8") in storage.deleted
        assert ("client-a", "device-1", "archives/sess-ready/playlist.m3u8") in storage.deleted

    async def test_marks_rows_deleted(self, service_and_pool):
        service, pool, _ = service_and_pool

        await service.cleanup_expired_archives()

        assert [args[0] for _, args in pool.executed] == [1, 2]
        assert all("SET status = 'deleted'" in query for query, _ in pool.executed)
