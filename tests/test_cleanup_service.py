"""Tests for CleanupService scan cost and interval configuration."""

import asyncio
import fnmatch
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from stream_processor.config.settings import ProcessingConfig, settings
from stream_processor.service.cleanup_service import CleanupService
from stream_processor.service.storage_backend import FileInfo, StorageBackend


class CountingStorage(StorageBackend):
    """In-memory backend that records how often the device list is scanned."""

    def __init__(self, files: dict[tuple[str, str], dict[str, list[FileInfo]]] | None = None):
        self.files = files or {}
        self.scan_count = 0
        self.deleted: list[tuple[str, str, str]] = []
        # Subpaths file_exists() should report as present.
        self.existing: set[tuple[str, str, str]] = set()

    def get_storage_type(self) -> str:
        return "counting"

    def ensure_directory_exists(self, client_id, device_id, subpath) -> None:
        pass

    def write_file(self, client_id, device_id, subpath, data, content_type=None) -> str:
        return subpath

    def write_file_atomic(self, client_id, device_id, subpath, data, content_type=None) -> str:
        return subpath

    def read_file(self, client_id, device_id, subpath) -> bytes | None:
        return None

    def file_exists(self, client_id, device_id, subpath) -> bool:
        return (client_id, device_id, subpath) in self.existing

    def delete_file(self, client_id, device_id, subpath) -> bool:
        self.deleted.append((client_id, device_id, subpath))
        return True

    def get_file_info(self, client_id, device_id, subpath) -> FileInfo | None:
        return None

    def list_files(self, client_id, device_id, subpath, pattern=None) -> Iterator[FileInfo]:
        for info in self.files.get((client_id, device_id), {}).get(subpath, []):
            # Honour the StorageBackend.list_files contract: without this the
            # frame cleanup's three extension patterns each match every file.
            if pattern is None or fnmatch.fnmatch(info.name, pattern):
                yield info

    def list_all_devices(self) -> Iterator[tuple[str, str]]:
        self.scan_count += 1
        yield from self.files.keys()

    def get_local_path(self, client_id, device_id, subpath) -> Path | None:
        return None

    def get_local_directory(self, client_id, device_id, subpath) -> Path | None:
        return None

    def cleanup_temp_files(self, max_age_seconds: int = 600) -> int:
        return 0


@pytest.fixture
def storage():
    old = time.time() - 48 * 3600
    return CountingStorage(
        {
            ("client-a", "device-1"): {
                "hls/segments": [FileInfo(name="seg_000001.ts", size=100, mtime=old)],
                "frames": [
                    FileInfo(name="f.jpg", size=10, mtime=old),
                    FileInfo(name="f.png", size=10, mtime=old),
                    FileInfo(name="keep.txt", size=10, mtime=old),
                ],
            },
            ("client-b", "device-2"): {
                "hls/segments": [FileInfo(name="seg_000002.ts", size=100, mtime=time.time())],
            },
        }
    )


class TestScanCost:
    async def test_cycle_scans_the_device_list_once(self, storage):
        """Segment cleanup and frame cleanup used to scan separately."""
        service = CleanupService(storage=storage)

        await service._run_cleanup()

        assert storage.scan_count == 1

    async def test_deletes_only_files_past_retention(self, storage):
        service = CleanupService(storage=storage)

        await service._run_cleanup()

        assert ("client-a", "device-1", "hls/segments/seg_000001.ts") in storage.deleted
        assert ("client-b", "device-2", "hls/segments/seg_000002.ts") not in storage.deleted
        # Once each, not once per extension pattern the frame cleanup tries.
        assert storage.deleted.count(("client-a", "device-1", "frames/f.jpg")) == 1
        assert storage.deleted.count(("client-a", "device-1", "frames/f.png")) == 1
        # Only the frame extensions are swept.
        assert ("client-a", "device-1", "frames/keep.txt") not in storage.deleted


class TestShutdown:
    async def test_stop_cuts_the_inter_cycle_wait_short(self, storage):
        """stop() must not have to outlast a whole interval."""
        service = CleanupService(storage=storage)
        service.cleanup_interval_seconds = 3600

        task = asyncio.create_task(service.run())
        for _ in range(200):
            if service.running:
                break
            await asyncio.sleep(0.01)
        assert service.running, "run() never armed; the rest of this test proves nothing"

        await service.stop()

        # Fails by timing out if run() is still sitting in the interval wait.
        await asyncio.wait_for(task, timeout=2)
        assert service.running is False

    async def test_stop_before_run_never_starts_a_cycle(self, storage):
        """A stop that lands before run() arms must not leave a spinning loop."""
        service = CleanupService(storage=storage)
        service.cleanup_interval_seconds = 3600

        await service.stop()

        # The wait returns instantly on an already-set event, so a loop entered
        # here would spin without delay. Timing out is the failure.
        await asyncio.wait_for(service.run(), timeout=2)
        assert storage.scan_count == 0
        assert service.running is False


@pytest.fixture
def defaults(monkeypatch):
    """ProcessingConfig with no env or .env override, so defaults are the defaults."""
    monkeypatch.delenv("PROCESSING_CLEANUP_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("PROCESSING_RETENTION_HOURS", raising=False)
    return ProcessingConfig(_env_file=None)


class TestInterval:
    def test_interval_comes_from_settings(self, storage):
        service = CleanupService(storage=storage)

        assert service.cleanup_interval_seconds == settings.processing.cleanup_interval_seconds

    def test_default_interval_is_one_hour(self, defaults):
        assert defaults.cleanup_interval_seconds == 3600

    def test_default_interval_stays_below_the_retention_window(self, defaults):
        assert defaults.cleanup_interval_seconds < defaults.retention_hours * 3600

    def test_interval_must_be_positive(self):
        """A zero interval would scan storage continuously."""
        with pytest.raises(ValidationError):
            ProcessingConfig(_env_file=None, cleanup_interval_seconds=0)
