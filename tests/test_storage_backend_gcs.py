"""Tests for GcsStorageBackend operation costs."""

import pytest

from stream_processor.service.storage_backend import GcsStorageBackend

from .fake_gcs import FakeBucket, FakeGcsClient


def build_objects(clients: int, devices_per_client: int, segments_per_device: int) -> dict:
    """Build a bucket layout of client_ids/{c}/device_id/{d}/hls/segments/seg_*.ts."""
    objects = {}
    for c in range(clients):
        for d in range(devices_per_client):
            base = f"client_ids/client-{c}/device_id/device-{d}"
            for s in range(segments_per_device):
                objects[f"{base}/hls/segments/seg_{s:06d}.ts"] = b"x"
    return objects


@pytest.fixture
def gcs(request):
    """A GcsStorageBackend wired to a fake client. Param: the object map."""
    objects = getattr(request, "param", build_objects(3, 4, 10))
    bucket = FakeBucket("test-bucket", objects)
    backend = GcsStorageBackend(bucket_name="test-bucket")
    backend._client = FakeGcsClient(bucket, page_size=1000)
    backend._bucket = bucket
    return backend, bucket


class TestListAllDevices:
    def test_returns_every_client_device_pair(self, gcs):
        backend, _ = gcs

        pairs = sorted(backend.list_all_devices())

        assert pairs == sorted((f"client-{c}", f"device-{d}") for c in range(3) for d in range(4))

    def test_scan_cost_is_one_op_per_client_plus_one(self, gcs):
        """The old implementation cost one op per ~1000 objects; this one does not."""
        backend, bucket = gcs
        bucket.class_a_ops = 0

        list(backend.list_all_devices())

        # 1 listing for client_ids/ + 1 per client for its device_id/ level.
        assert bucket.class_a_ops == 1 + 3

    def test_scan_cost_does_not_grow_with_object_count(self):
        """2,300 pages of objects used to mean 2,300 Class A ops per scan."""
        small = FakeBucket("b", build_objects(2, 2, 5))
        large = FakeBucket("b", build_objects(2, 2, 5000))

        costs = []
        for bucket in (small, large):
            backend = GcsStorageBackend(bucket_name="b")
            backend._client = FakeGcsClient(bucket, page_size=1000)
            backend._bucket = bucket
            list(backend.list_all_devices())
            costs.append(bucket.class_a_ops)

        assert costs[0] == costs[1] == 3
        assert len(large.objects) > 1000  # would have paginated under the old code

    def test_a_level_over_one_page_of_prefixes_costs_more(self):
        """GCS counts prefixes toward the page size, so 1 + N_clients is a floor."""
        objects = {
            f"client_ids/client-0/device_id/device-{d:05d}/hls/segments/seg_000001.ts"
            for d in range(2500)
        }
        bucket = FakeBucket("b", dict.fromkeys(objects, b"x"))
        backend = GcsStorageBackend(bucket_name="b")
        backend._client = FakeGcsClient(bucket, page_size=1000)
        backend._bucket = bucket

        pairs = list(backend.list_all_devices())

        assert len(pairs) == 2500
        # 1 page for client_ids/ + 3 pages for 2500 device prefixes.
        assert bucket.class_a_ops == 4

    def test_ignores_objects_outside_the_expected_layout(self, gcs):
        backend, bucket = gcs
        bucket.objects["client_ids/stray-file.txt"] = b"x"
        bucket.objects["unrelated/thing.ts"] = b"x"

        pairs = list(backend.list_all_devices())

        assert ("stray-file.txt", "") not in pairs
        assert len(pairs) == 12

    def test_empty_bucket_yields_nothing(self):
        bucket = FakeBucket("b", {})
        backend = GcsStorageBackend(bucket_name="b")
        backend._client = FakeGcsClient(bucket)
        backend._bucket = bucket

        assert list(backend.list_all_devices()) == []


class TestBlobOperationCosts:
    def test_read_file_costs_one_class_b_op(self, gcs):
        backend, bucket = gcs
        bucket.objects["client_ids/c/device_id/d/hls/playlist.m3u8"] = b"#EXTM3U"
        bucket.class_b_ops = 0

        data = backend.read_file("c", "d", "hls/playlist.m3u8")

        assert data == b"#EXTM3U"
        assert bucket.class_b_ops == 1

    def test_read_file_returns_none_when_missing(self, gcs):
        backend, bucket = gcs
        bucket.class_b_ops = 0

        assert backend.read_file("c", "d", "nope.ts") is None
        assert bucket.class_b_ops == 1

    def test_delete_file_costs_no_class_b_op(self, gcs):
        backend, bucket = gcs
        key = "client_ids/client-0/device_id/device-0/hls/segments/seg_000000.ts"
        bucket.class_b_ops = 0

        assert backend.delete_file("client-0", "device-0", "hls/segments/seg_000000.ts") is True
        assert key not in bucket.objects
        assert bucket.class_b_ops == 0

    def test_delete_file_returns_false_when_missing(self, gcs):
        backend, bucket = gcs
        bucket.class_b_ops = 0

        assert backend.delete_file("client-0", "device-0", "hls/segments/gone.ts") is False
        assert bucket.class_b_ops == 0

    def test_get_file_info_costs_one_class_b_op(self, gcs):
        backend, bucket = gcs
        bucket.objects["client_ids/c/device_id/d/frames/f.jpg"] = b"1234"
        bucket.class_b_ops = 0

        info = backend.get_file_info("c", "d", "frames/f.jpg")

        assert info is not None
        assert info.name == "f.jpg"
        assert info.size == 4
        assert bucket.class_b_ops == 1

    def test_get_file_info_returns_none_when_missing(self, gcs):
        backend, bucket = gcs

        assert backend.get_file_info("c", "d", "frames/missing.jpg") is None
