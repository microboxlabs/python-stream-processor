#!/usr/bin/env python3
"""
Measure the Class A cost of one device scan against a real GCS bucket.

Read-only. Counts the list pages fetched by
`GcsStorageBackend.list_all_devices()` — one page is one Class A operation —
and projects the daily cost at the configured cleanup interval.

Usage:
    python scripts/gcs_scan_cost.py                    # uses STORAGE_GCS_BUCKET
    python scripts/gcs_scan_cost.py --bucket my-bucket --project my-project
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from stream_processor.config.settings import settings  # noqa: E402
from stream_processor.service.storage_backend import GcsStorageBackend  # noqa: E402

# https://cloud.google.com/storage/pricing — Standard, multi-region.
CLASS_A_USD_PER_1000 = 0.005


class CountingIterator:
    """Wraps a list_blobs iterator and counts the pages actually fetched."""

    def __init__(self, inner, counter):
        self._inner = inner
        self._counter = counter

    @property
    def pages(self):
        for page in self._inner.pages:
            self._counter["pages"] += 1
            yield page

    def __iter__(self):
        for page in self.pages:
            yield from page

    def __getattr__(self, name):
        return getattr(self._inner, name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=settings.storage.gcs_bucket)
    parser.add_argument("--project", default=settings.storage.gcs_project_id)
    parser.add_argument(
        "--interval",
        type=int,
        default=settings.processing.cleanup_interval_seconds,
        help="Cleanup interval in seconds used for the daily projection",
    )
    args = parser.parse_args()

    if not args.bucket:
        parser.error("no bucket: pass --bucket or set STORAGE_GCS_BUCKET")

    backend = GcsStorageBackend(bucket_name=args.bucket, project_id=args.project)
    counter = {"pages": 0}

    original_list_blobs = backend.client.list_blobs
    backend.client.list_blobs = lambda *a, **kw: CountingIterator(
        original_list_blobs(*a, **kw), counter
    )

    started = time.time()
    devices = list(backend.list_all_devices())
    elapsed = time.time() - started

    scans_per_day = 86400 / args.interval
    ops_per_day = counter["pages"] * scans_per_day

    print(f"bucket:            {args.bucket}")
    print(f"devices found:     {len(devices)}")
    print(f"class A ops/scan:  {counter['pages']}")
    print(f"scan duration:     {elapsed:.1f}s")
    print(f"cleanup interval:  {args.interval}s ({scans_per_day:.0f} scans/day)")
    print(f"class A ops/day:   {ops_per_day:,.0f}")
    print(f"projected USD/day: {ops_per_day / 1000 * CLASS_A_USD_PER_1000:.2f}")
    print()
    print("The cleanup service runs one scan per cycle. Two scans per cycle, or")
    print("an op count that tracks object count rather than client count, means")
    print("the prefix walk regressed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
