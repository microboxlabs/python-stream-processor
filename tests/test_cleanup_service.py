"""Tests for CleanupService scan cost and interval configuration."""

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from stream_processor.config.settings import settings
from stream_processor.service.cleanup_service import CleanupService
from stream_processor.service.storage_backend import FileInfo, StorageBackend


class CountingStorage(StorageBackend):
    """In-memory backend that records how often the device list is scanned."""

    def __init__(self, files: dict[tuple[str, str], dict[str, FileInfo]] | None = None):
        self.files = files or {}
        self.scan_count = 0
        self.deleted: list[tuple[str, str, str]] = []

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
        return False

    def delete_file(self, client_id, device_id, subpath) -> bool:
        self.deleted.append((client_id, device_id, subpath))
        return True

    def get_file_info(self, client_id, device_id, subpath) -> FileInfo | None:
        return None

    def list_files(self, client_id, device_id, subpath, pattern=None) -> Iterator[FileInfo]:
        yield from self.files.get((client_id, device_id), {}).get(subpath, [])

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
                "frames": [FileInfo(name="f.jpg", size=10, mtime=old)],
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
        assert ("client-a", "device-1", "frames/f.jpg") in storage.deleted
        assert ("client-b", "device-2", "hls/segments/seg_000002.ts") not in storage.deleted


class TestInterval:
    def test_interval_comes_from_settings(self, storage):
        service = CleanupService(storage=storage)

        assert service.cleanup_interval_seconds == settings.processing.cleanup_interval_seconds

    def test_default_interval_is_one_hour(self):
        assert settings.processing.cleanup_interval_seconds == 3600

    def test_interval_stays_below_the_retention_window(self):
        assert (
            settings.processing.cleanup_interval_seconds
            < settings.processing.retention_hours * 3600
        )
