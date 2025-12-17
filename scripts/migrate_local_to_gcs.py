#!/usr/bin/env python3
"""
Migration script to upload local storage data to GCS and create deferred transmissions.

Usage:
    python scripts/migrate_local_to_gcs.py --source /path/to/local/storage --dry-run
    python scripts/migrate_local_to_gcs.py --source /path/to/local/storage

Environment variables required:
    STORAGE_GCS_BUCKET - GCS bucket name
    ARCHIVE_DATABASE_URL - PostgreSQL connection URL
"""

import argparse
import asyncio
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
from google.cloud import storage


def parse_frame_timestamp(filename: str) -> datetime | None:
    """Parse timestamp from frame filename like 20251216_112452_569_000.jpg"""
    match = re.match(r"(\d{8})_(\d{6})_(\d{3})_\d+\.jpg", filename)
    if not match:
        return None
    date_str, time_str, millis = match.groups()
    try:
        dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
        dt = dt.replace(microsecond=int(millis) * 1000, tzinfo=UTC)
        return dt
    except ValueError:
        return None


def parse_segment_number(filename: str) -> int | None:
    """Parse segment number from filename like seg_000983.ts"""
    match = re.match(r"seg_(\d+)\.ts", filename)
    if match:
        return int(match.group(1))
    return None


def discover_devices(source_path: Path) -> list[dict]:
    """Discover all devices with data in the source storage."""
    devices = []
    client_ids_path = source_path / "client_ids"

    if not client_ids_path.exists():
        print(f"No client_ids directory found at {client_ids_path}")
        return devices

    for client_dir in client_ids_path.iterdir():
        if not client_dir.is_dir():
            continue
        client_id = client_dir.name

        device_id_path = client_dir / "device_id"
        if not device_id_path.exists():
            continue

        for device_dir in device_id_path.iterdir():
            if not device_dir.is_dir():
                continue
            device_id = device_dir.name

            # Check for actual content
            frames_path = device_dir / "frames"
            hls_path = device_dir / "hls"
            segments_path = hls_path / "segments"

            frame_files = list(frames_path.glob("*.jpg")) if frames_path.exists() else []
            segment_files = list(segments_path.glob("*.ts")) if segments_path.exists() else []

            if frame_files or segment_files:
                devices.append(
                    {
                        "client_id": client_id,
                        "device_id": device_id,
                        "path": device_dir,
                        "frames_path": frames_path,
                        "segments_path": segments_path,
                        "frame_count": len(frame_files),
                        "segment_count": len(segment_files),
                    }
                )

    return devices


def analyze_segments(segments_path: Path) -> dict:
    """Analyze segment files to determine session info."""
    if not segments_path.exists():
        return {}

    segment_files = list(segments_path.glob("*.ts"))
    if not segment_files:
        return {}

    # Parse segment numbers and get file times
    segments = []
    for seg_file in segment_files:
        seg_num = parse_segment_number(seg_file.name)
        if seg_num is not None:
            stat = seg_file.stat()
            segments.append(
                {
                    "number": seg_num,
                    "path": seg_file,
                    "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                    "size": stat.st_size,
                }
            )

    if not segments:
        return {}

    segments.sort(key=lambda x: x["number"])

    # Group into sessions (gaps > 5 segments indicate different sessions)
    sessions = []
    current_session = [segments[0]]

    for i in range(1, len(segments)):
        gap = segments[i]["number"] - segments[i - 1]["number"]
        if gap > 5:
            # New session
            sessions.append(current_session)
            current_session = [segments[i]]
        else:
            current_session.append(segments[i])

    if current_session:
        sessions.append(current_session)

    return {
        "total_segments": len(segments),
        "first_segment": segments[0]["number"],
        "last_segment": segments[-1]["number"],
        "sessions": sessions,
    }


def analyze_frames(frames_path: Path) -> dict:
    """Analyze frame files to determine time range."""
    if not frames_path.exists():
        return {}

    frame_files = list(frames_path.glob("*.jpg"))
    if not frame_files:
        return {}

    timestamps = []
    for frame_file in frame_files:
        ts = parse_frame_timestamp(frame_file.name)
        if ts:
            timestamps.append(ts)

    if not timestamps:
        return {}

    timestamps.sort()

    return {
        "total_frames": len(timestamps),
        "earliest": timestamps[0],
        "latest": timestamps[-1],
        "duration_seconds": (timestamps[-1] - timestamps[0]).total_seconds(),
    }


async def upload_to_gcs(
    source_path: Path,
    devices: list[dict],
    bucket_name: str,
    dry_run: bool = True,
) -> dict:
    """Upload files to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    stats = {"uploaded": 0, "skipped": 0, "errors": 0, "bytes": 0}

    for device in devices:
        client_id = device["client_id"]
        device_id = device["device_id"]

        print(f"\n{'[DRY-RUN] ' if dry_run else ''}Processing {client_id}/{device_id}")

        # Upload frames
        if device["frames_path"].exists():
            for frame_file in device["frames_path"].glob("*.jpg"):
                gcs_path = f"client_ids/{client_id}/device_id/{device_id}/frames/{frame_file.name}"

                if dry_run:
                    print(f"  Would upload: {frame_file.name} -> {gcs_path}")
                    stats["skipped"] += 1
                else:
                    try:
                        blob = bucket.blob(gcs_path)
                        if not blob.exists():
                            blob.upload_from_filename(str(frame_file), content_type="image/jpeg")
                            stats["uploaded"] += 1
                            stats["bytes"] += frame_file.stat().st_size
                        else:
                            stats["skipped"] += 1
                    except Exception as e:
                        print(f"  Error uploading {frame_file.name}: {e}")
                        stats["errors"] += 1

        # Upload segments
        if device["segments_path"].exists():
            for seg_file in device["segments_path"].glob("*.ts"):
                gcs_path = (
                    f"client_ids/{client_id}/device_id/{device_id}/hls/segments/{seg_file.name}"
                )

                if dry_run:
                    print(f"  Would upload: {seg_file.name} -> {gcs_path}")
                    stats["skipped"] += 1
                else:
                    try:
                        blob = bucket.blob(gcs_path)
                        if not blob.exists():
                            blob.upload_from_filename(str(seg_file), content_type="video/mp2t")
                            stats["uploaded"] += 1
                            stats["bytes"] += seg_file.stat().st_size
                        else:
                            stats["skipped"] += 1
                    except Exception as e:
                        print(f"  Error uploading {seg_file.name}: {e}")
                        stats["errors"] += 1

    return stats


async def create_deferred_transmissions(
    devices: list[dict],
    bucket_name: str,
    database_url: str,
    segment_duration: int = 30,
    retention_days: int = 7,
    dry_run: bool = True,
) -> list[dict]:
    """Create deferred transmission archives from existing segments."""
    created_archives = []

    if dry_run:
        print("\n[DRY-RUN] Would create deferred transmissions:")
    else:
        conn = await asyncpg.connect(database_url)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    try:
        for device in devices:
            client_id = device["client_id"]
            device_id = device["device_id"]

            segment_info = analyze_segments(device["segments_path"])
            if not segment_info or not segment_info.get("sessions"):
                print(f"  No segments found for {client_id}/{device_id}")
                continue

            frame_info = analyze_frames(device["frames_path"])

            # Create an archive for each session
            for session_segments in segment_info["sessions"]:
                if len(session_segments) < 2:
                    continue

                session_id = str(uuid.uuid4())
                first_seg = session_segments[0]
                last_seg = session_segments[-1]

                # Estimate start/end times from segment mtimes and frame timestamps
                if frame_info:
                    started_at = frame_info["earliest"]
                    ended_at = frame_info["latest"]
                else:
                    # Fall back to segment file modification times
                    started_at = first_seg["mtime"]
                    ended_at = last_seg["mtime"]

                duration_seconds = int((ended_at - started_at).total_seconds())
                if duration_seconds < 60:
                    duration_seconds = len(session_segments) * segment_duration

                expires_at = datetime.now(UTC) + timedelta(days=retention_days)
                archive_path = f"archives/{session_id}"

                archive_info = {
                    "session_id": session_id,
                    "client_id": client_id,
                    "device_id": device_id,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "duration_seconds": duration_seconds,
                    "first_segment": first_seg["number"],
                    "last_segment": last_seg["number"],
                    "segment_count": len(session_segments),
                    "archive_path": archive_path,
                    "expires_at": expires_at,
                }

                if dry_run:
                    print(f"\n  Session: {session_id}")
                    print(f"    Client: {client_id}, Device: {device_id}")
                    print(f"    Started: {started_at}, Ended: {ended_at}")
                    print(f"    Duration: {duration_seconds}s, Segments: {len(session_segments)}")
                    print(f"    Segment range: {first_seg['number']} - {last_seg['number']}")
                else:
                    # Copy segments to archive directory
                    print(f"\n  Creating archive {session_id}")

                    for seg in session_segments:
                        src_path = f"client_ids/{client_id}/device_id/{device_id}/hls/segments/{seg['path'].name}"
                        dst_path = f"client_ids/{client_id}/device_id/{device_id}/{archive_path}/segments/{seg['path'].name}"

                        src_blob = bucket.blob(src_path)
                        dst_blob = bucket.blob(dst_path)

                        if src_blob.exists() and not dst_blob.exists():
                            bucket.copy_blob(src_blob, bucket, dst_path)

                    # Generate VOD playlist
                    playlist_lines = [
                        "#EXTM3U",
                        "#EXT-X-VERSION:3",
                        "#EXT-X-TARGETDURATION:" + str(segment_duration),
                        "#EXT-X-MEDIA-SEQUENCE:" + str(first_seg["number"]),
                        "#EXT-X-PLAYLIST-TYPE:VOD",
                    ]

                    for seg in session_segments:
                        playlist_lines.append(f"#EXTINF:{segment_duration}.0,")
                        playlist_lines.append(f"segments/{seg['path'].name}")

                    playlist_lines.append("#EXT-X-ENDLIST")
                    playlist_content = "\n".join(playlist_lines)

                    playlist_path = (
                        f"client_ids/{client_id}/device_id/{device_id}/{archive_path}/playlist.m3u8"
                    )
                    playlist_blob = bucket.blob(playlist_path)
                    playlist_blob.upload_from_string(
                        playlist_content, content_type="application/vnd.apple.mpegurl"
                    )

                    # Insert into database
                    await conn.execute(
                        """
                        INSERT INTO deferred_transmissions (
                            client_id, device_id, session_id, started_at, ended_at,
                            duration_seconds, first_segment_number, last_segment_number,
                            segment_count, archive_path, status, expires_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'ready', $11)
                        ON CONFLICT (client_id, device_id, session_id) DO NOTHING
                        """,
                        client_id,
                        device_id,
                        session_id,
                        started_at,
                        ended_at,
                        duration_seconds,
                        first_seg["number"],
                        last_seg["number"],
                        len(session_segments),
                        archive_path,
                        expires_at,
                    )

                    print(f"    Copied {len(session_segments)} segments, created playlist")

                created_archives.append(archive_info)

    finally:
        if not dry_run:
            await conn.close()

    return created_archives


async def main():
    parser = argparse.ArgumentParser(description="Migrate local storage to GCS")
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Source storage path (containing client_ids directory)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip GCS upload, only create archives",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Archive retention period in days (default: 7)",
    )
    parser.add_argument(
        "--segment-duration",
        type=int,
        default=30,
        help="Segment duration in seconds (default: 30)",
    )
    args = parser.parse_args()

    # Get config from environment
    bucket_name = os.environ.get("STORAGE_GCS_BUCKET")
    database_url = os.environ.get("ARCHIVE_DATABASE_URL")

    if not bucket_name:
        print("Error: STORAGE_GCS_BUCKET environment variable required")
        return 1

    if not database_url and not args.dry_run:
        print("Error: ARCHIVE_DATABASE_URL environment variable required")
        return 1

    print(f"Source: {args.source}")
    print(f"GCS Bucket: {bucket_name}")
    print(f"Database: {database_url[:50]}..." if database_url else "Database: N/A")
    print(f"Dry run: {args.dry_run}")

    # Discover devices
    print("\n=== Discovering devices ===")
    devices = discover_devices(args.source)

    if not devices:
        print("No devices with data found")
        return 0

    for device in devices:
        print(f"\nDevice: {device['client_id']}/{device['device_id']}")
        print(f"  Frames: {device['frame_count']}")
        print(f"  Segments: {device['segment_count']}")

        segment_info = analyze_segments(device["segments_path"])
        if segment_info:
            print(
                f"  Segment range: {segment_info['first_segment']} - {segment_info['last_segment']}"
            )
            print(f"  Sessions: {len(segment_info.get('sessions', []))}")

        frame_info = analyze_frames(device["frames_path"])
        if frame_info:
            print(f"  Time range: {frame_info['earliest']} to {frame_info['latest']}")
            print(f"  Duration: {frame_info['duration_seconds']:.0f}s")

    # Upload to GCS
    if not args.skip_upload:
        print("\n=== Uploading to GCS ===")
        upload_stats = await upload_to_gcs(args.source, devices, bucket_name, args.dry_run)
        print(f"\nUpload stats: {upload_stats}")

    # Create deferred transmissions
    print("\n=== Creating deferred transmissions ===")
    archives = await create_deferred_transmissions(
        devices,
        bucket_name,
        database_url or "",
        args.segment_duration,
        args.retention_days,
        args.dry_run,
    )
    print(f"\nCreated {len(archives)} archive(s)")

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
