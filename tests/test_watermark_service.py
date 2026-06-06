"""Tests for WatermarkService local-temp GCS path (no re-upload). Pillow only — CI-safe."""

import io
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from PIL import Image

import stream_processor.service.watermark_service as wm
from stream_processor.config.settings import WatermarkConfig


def _jpeg_bytes(w=64, h=48):
    buf = io.BytesIO()
    Image.new("RGB", (w, h), "gray").save(buf, "JPEG")
    return buf.getvalue()


def _service(monkeypatch):
    storage = MagicMock()
    # Avoid building a real (mkdir-ing) storage backend.
    monkeypatch.setattr(wm, "create_storage_backend", lambda **kw: storage)
    svc = wm.WatermarkService(WatermarkConfig(enabled=True))
    return svc, storage


class TestWatermarkGcsLocalTemp:
    def test_writes_local_temp_and_does_not_upload(self, monkeypatch):
        svc, storage = _service(monkeypatch)
        storage.read_file.return_value = _jpeg_bytes()

        path = svc._add_watermark_gcs(
            "gs://bucket/client_ids/c/device_id/d/frames/x.jpg",
            datetime(2026, 6, 6, 21, 0, 0, tzinfo=UTC),
        )
        try:
            # Returns a local temp under wm_dir — not the gs:// URI.
            assert svc.is_watermark_temp(path)
            assert Path(path).exists()
            with Image.open(path) as im:
                assert im.size == (64, 48)  # original frame untouched in size
            # The original GCS object is NOT overwritten.
            storage.write_file.assert_not_called()
        finally:
            svc.cleanup_temp_frames([path])

        # cleanup removed the temp.
        assert not Path(path).exists()

    def test_is_watermark_temp_excludes_originals(self, monkeypatch):
        svc, _ = _service(monkeypatch)
        assert svc.is_watermark_temp("gs://bucket/client_ids/c/device_id/d/frames/x.jpg") is False
        assert svc.is_watermark_temp("/storage/streams/c/d/frames/x.jpg") is False

    def test_cleanup_ignores_non_temp_paths(self, monkeypatch):
        svc, _ = _service(monkeypatch)
        # Must not raise or attempt to delete originals / GCS URIs.
        svc.cleanup_temp_frames(["gs://b/x.jpg", "/some/original/frame.jpg"])
