#!/usr/bin/env python3
"""
Measure the Class A cost of one device scan against a real GCS bucket.

Read-only. Counts the list pages `GcsStorageBackend.list_all_devices()`
fetches — one page is one Class A operation — and projects the daily cost at
the configured cleanup interval. Pagination is included: a level holding more
than 1000 prefixes costs the extra pages it takes to read.

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


class CountingBackend(GcsStorageBackend):
    """A GCS backend that records how many list pages a scan fetches."""

    def __init__(self, bucket_name: str, project_id: str | None = None):
        super().__init__(bucket_name, project_id)
        self.pages = 0

    def _walk_pages(self, iterator):
        for page in super()._walk_pages(iterator):
            self.pages += 1
            yield page


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

    bucket: str | None = args.bucket
    if not bucket:
        print("no bucket: pass --bucket or set STORAGE_GCS_BUCKET", file=sys.stderr)
        return 2

    interval: int = args.interval
    if interval <= 0:
        print("--interval must be greater than zero", file=sys.stderr)
        return 2

    backend = CountingBackend(bucket, args.project)

    started = time.time()
    devices = list(backend.list_all_devices())
    elapsed = time.time() - started

    scans_per_day = 86400 / interval
    ops_per_day = backend.pages * scans_per_day

    print(f"bucket:            {bucket}")
    print(f"devices found:     {len(devices)}")
    print(f"class A ops/scan:  {backend.pages}")
    print(f"scan duration:     {elapsed:.1f}s")
    print(f"cleanup interval:  {interval}s ({scans_per_day:.0f} scans/day)")
    print(f"class A ops/day:   {ops_per_day:,.0f}")
    print(f"projected USD/day: {ops_per_day / 1000 * CLASS_A_USD_PER_1000:.2f}")
    print()
    print("Expect one listing for client_ids/ plus one per client, and one more")
    print("op per 1000 prefixes in any single listing. An op count in the")
    print("thousands means the scan is enumerating objects again.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
