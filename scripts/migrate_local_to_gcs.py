#!/usr/bin/env python3
"""
Migration script to upload local storage data to GCS and create deferred transmissions.

Handles symlink-based device sharing:
- Owner clients have real device directories
- Shared clients have symlinks pointing to owner's devices
- Data is uploaded once (from owner), but DB entries are created for all clients

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
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
from google.cloud import storage


@dataclass
class DeviceInfo:
    """Information about a device and its data."""

    owner_client_id: str
    device_id: str
    path: Path  # Real path (resolved symlinks)
    frames_path: Path
    segments_path: Path
    frame_count: int
    segment_count: int
    # Clients that share this device (including owner)
    shared_clients: list[str] = field(default_factory=list)
    # Secondary keys pointing to this device (client_id -> list of keys)
    secondary_keys: dict[str, list[str]] = field(default_factory=dict)
    # Request IDs pointing to this device (client_id -> list of request_ids)
    request_ids: dict[str, list[str]] = field(default_factory=dict)


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


def resolve_symlink_target(symlink_path: Path, base_path: Path) -> tuple[str, str] | None:
    """
    Resolve a device symlink to get owner_client_id and device_id.

    Returns (owner_client_id, device_id) or None if not resolvable.
    """
    try:
        # Get the symlink target (e.g., "../../PdMZi.../device_id/2c_f7_f1...")
        target = os.readlink(symlink_path)

        # Resolve to absolute path
        resolved = (symlink_path.parent / target).resolve()

        # Extract client_id and device_id from path
        # Path format: .../client_ids/{client_id}/device_id/{device_id}
        parts = resolved.parts
        for i, part in enumerate(parts):
            if part == "client_ids" and i + 3 < len(parts):
                if parts[i + 2] == "device_id":
                    return parts[i + 1], parts[i + 3]
        return None
    except (OSError, ValueError):
        return None


def discover_devices(source_path: Path) -> dict[str, DeviceInfo]:
    """
    Discover all devices with data in the source storage.

    Returns a dict keyed by "{owner_client_id}:{device_id}" with DeviceInfo.
    Handles symlinks to avoid duplicates.
    """
    devices: dict[str, DeviceInfo] = {}
    client_ids_path = source_path / "client_ids"

    if not client_ids_path.exists():
        print(f"No client_ids directory found at {client_ids_path}")
        return devices

    # First pass: find all real device directories (owners)
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

            # Check if this is a symlink (shared device) or real directory (owner)
            if device_dir.is_symlink():
                # This is a shared device - will process in second pass
                continue

            # This is a real directory (owner)
            frames_path = device_dir / "frames"
            hls_path = device_dir / "hls"
            segments_path = hls_path / "segments"

            frame_files = list(frames_path.glob("*.jpg")) if frames_path.exists() else []
            segment_files = list(segments_path.glob("*.ts")) if segments_path.exists() else []

            if frame_files or segment_files:
                key = f"{client_id}:{device_id}"
                devices[key] = DeviceInfo(
                    owner_client_id=client_id,
                    device_id=device_id,
                    path=device_dir,
                    frames_path=frames_path,
                    segments_path=segments_path,
                    frame_count=len(frame_files),
                    segment_count=len(segment_files),
                    shared_clients=[client_id],  # Owner is always included
                    secondary_keys={},
                    request_ids={},
                )

    # Second pass: find shared devices (symlinks) and secondary_key/request_id mappings
    for client_dir in client_ids_path.iterdir():
        if not client_dir.is_dir():
            continue
        client_id = client_dir.name

        # Check device_id symlinks
        device_id_path = client_dir / "device_id"
        if device_id_path.exists():
            for device_entry in device_id_path.iterdir():
                if device_entry.is_symlink():
                    result = resolve_symlink_target(device_entry, source_path)
                    if result:
                        owner_client_id, device_id = result
                        key = f"{owner_client_id}:{device_id}"
                        if key in devices:
                            if client_id not in devices[key].shared_clients:
                                devices[key].shared_clients.append(client_id)

        # Check secondary_key symlinks
        secondary_key_path = client_dir / "secondary_key"
        if secondary_key_path.exists():
            for key_entry in secondary_key_path.iterdir():
                if key_entry.is_symlink():
                    # secondary_key links to ../device_id/{device_id}
                    try:
                        target = os.readlink(key_entry)
                        # Extract device_id from target like "../device_id/2c_f7_f1..."
                        if "../device_id/" in target:
                            device_id = target.split("../device_id/")[1]
                            # Find the owner for this device
                            for dev_info in devices.values():
                                if dev_info.device_id == device_id:
                                    if client_id not in dev_info.secondary_keys:
                                        dev_info.secondary_keys[client_id] = []
                                    dev_info.secondary_keys[client_id].append(key_entry.name)
                                    break
                    except (OSError, IndexError):
                        pass

        # Check request_id symlinks
        # request_id_path = client_dir / "request_id"
        # if request_id_path.exists():
        #     for req_entry in request_id_path.iterdir():
        #         if req_entry.is_symlink():
        #             try:
        #                 target = os.readlink(req_entry)
        #                 if "../device_id/" in target:
        #                     device_id = target.split("../device_id/")[1]
        #                     for dev_info in devices.values():
        #                         if dev_info.device_id == device_id:
        #                             if client_id not in dev_info.request_ids:
        #                                 dev_info.request_ids[client_id] = []
        #                             dev_info.request_ids[client_id].append(req_entry.name)
        #                             break
        #             except (OSError, IndexError):
        #                 pass

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
    devices: dict[str, DeviceInfo],
    bucket_name: str,
    dry_run: bool = True,
) -> dict:
    """
    Upload files to GCS.

    Only uploads from owner's real directories (not symlinks).
    """
    stats = {"uploaded": 0, "skipped": 0, "errors": 0, "bytes": 0}

    if not dry_run:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
    else:
        bucket = None

    for device in devices.values():
        owner_client_id = device.owner_client_id
        device_id = device.device_id

        print(f"\n{'[DRY-RUN] ' if dry_run else ''}Uploading {owner_client_id}/{device_id}")
        print(f"  Shared with: {device.shared_clients}")

        # Upload frames (only from owner's path)
        if device.frames_path.exists():
            frame_count = 0
            for frame_file in device.frames_path.glob("*.jpg"):
                gcs_path = (
                    f"client_ids/{owner_client_id}/device_id/{device_id}/frames/{frame_file.name}"
                )

                if dry_run:
                    frame_count += 1
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
            if dry_run:
                print(f"  Would upload {frame_count} frames")
                stats["skipped"] += frame_count

        # Upload segments (only from owner's path)
        if device.segments_path.exists():
            seg_count = 0
            for seg_file in device.segments_path.glob("*.ts"):
                gcs_path = f"client_ids/{owner_client_id}/device_id/{device_id}/hls/segments/{seg_file.name}"

                if dry_run:
                    seg_count += 1
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
            if dry_run:
                print(f"  Would upload {seg_count} segments")
                stats["skipped"] += seg_count

    return stats


async def create_deferred_transmissions(
    devices: dict[str, DeviceInfo],
    bucket_name: str,
    database_url: str,
    segment_duration: int = 30,
    retention_days: int = 7,
    dry_run: bool = True,
) -> list[dict]:
    """
    Create deferred transmission archives from existing segments.

    Creates archive files once (for owner), then creates DB entries for ALL
    clients that share the device.
    """
    created_archives = []

    if dry_run:
        print("\n[DRY-RUN] Would create deferred transmissions:")
        conn = None
        bucket = None
    else:
        conn = await asyncpg.connect(database_url)
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)

    try:
        for device in devices.values():
            owner_client_id = device.owner_client_id
            device_id = device.device_id

            segment_info = analyze_segments(device.segments_path)
            if not segment_info or not segment_info.get("sessions"):
                print(f"  No segments found for {owner_client_id}/{device_id}")
                continue

            frame_info = analyze_frames(device.frames_path)

            # Create an archive for each session
            for session_segments in segment_info["sessions"]:
                if len(session_segments) < 2:
                    continue

                session_id = str(uuid.uuid4())
                first_seg = session_segments[0]
                last_seg = session_segments[-1]

                # Estimate start/end times
                if frame_info:
                    started_at = frame_info["earliest"]
                    ended_at = frame_info["latest"]
                else:
                    started_at = first_seg["mtime"]
                    ended_at = last_seg["mtime"]

                duration_seconds = int((ended_at - started_at).total_seconds())
                if duration_seconds < 60:
                    duration_seconds = len(session_segments) * segment_duration

                expires_at = datetime.now(UTC) + timedelta(days=retention_days)
                archive_path = f"archives/{session_id}"

                archive_info = {
                    "session_id": session_id,
                    "owner_client_id": owner_client_id,
                    "device_id": device_id,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "duration_seconds": duration_seconds,
                    "first_segment": first_seg["number"],
                    "last_segment": last_seg["number"],
                    "segment_count": len(session_segments),
                    "archive_path": archive_path,
                    "shared_clients": device.shared_clients,
                }

                if dry_run:
                    print(f"\n  Session: {session_id}")
                    print(f"    Owner: {owner_client_id}, Device: {device_id}")
                    print(f"    Started: {started_at}, Ended: {ended_at}")
                    print(f"    Duration: {duration_seconds}s, Segments: {len(session_segments)}")
                    print(f"    Shared with clients: {device.shared_clients}")
                    print(f"    Would create DB entries for {len(device.shared_clients)} client(s)")
                else:
                    print(f"\n  Creating archive {session_id}")

                    # Copy segments to archive directory (once, for owner only)
                    for seg in session_segments:
                        src_path = f"client_ids/{owner_client_id}/device_id/{device_id}/hls/segments/{seg['path'].name}"
                        dst_path = f"client_ids/{owner_client_id}/device_id/{device_id}/{archive_path}/segments/{seg['path'].name}"

                        src_blob = bucket.blob(src_path)
                        dst_blob = bucket.blob(dst_path)

                        if src_blob.exists() and not dst_blob.exists():
                            bucket.copy_blob(src_blob, bucket, dst_path)

                    # Generate VOD playlist (once, for owner only)
                    playlist_lines = [
                        "#EXTM3U",
                        "#EXT-X-VERSION:3",
                        f"#EXT-X-TARGETDURATION:{segment_duration}",
                        f"#EXT-X-MEDIA-SEQUENCE:{first_seg['number']}",
                        "#EXT-X-PLAYLIST-TYPE:VOD",
                    ]

                    for seg in session_segments:
                        playlist_lines.append(f"#EXTINF:{segment_duration}.0,")
                        playlist_lines.append(f"segments/{seg['path'].name}")

                    playlist_lines.append("#EXT-X-ENDLIST")
                    playlist_content = "\n".join(playlist_lines)

                    playlist_path = f"client_ids/{owner_client_id}/device_id/{device_id}/{archive_path}/playlist.m3u8"
                    playlist_blob = bucket.blob(playlist_path)
                    playlist_blob.upload_from_string(
                        playlist_content, content_type="application/vnd.apple.mpegurl"
                    )

                    print(f"    Copied {len(session_segments)} segments, created playlist")

                    # Create DB entries for ALL clients that share this device
                    for client_id in device.shared_clients:
                        await conn.execute(
                            """
                            INSERT INTO deferred_transmissions (
                                client_id, device_id, session_id, owner_client_id,
                                started_at, ended_at, duration_seconds,
                                first_segment_number, last_segment_number,
                                segment_count, archive_path, status, expires_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'ready', $12)
                            ON CONFLICT (client_id, device_id, session_id) DO NOTHING
                            """,
                            client_id,
                            device_id,
                            session_id,
                            owner_client_id,  # Always use the owner's client_id for storage path
                            started_at,
                            ended_at,
                            duration_seconds,
                            first_seg["number"],
                            last_seg["number"],
                            len(session_segments),
                            archive_path,
                            expires_at,
                        )
                        print(f"    Created DB entry for client: {client_id}")

                created_archives.append(archive_info)

    finally:
        if conn:
            await conn.close()

    return created_archives


async def populate_symlink_tables(
    devices: dict[str, DeviceInfo],
    database_url: str,
    dry_run: bool = True,
) -> dict:
    """
    Populate stream_shared_devices and stream_index_mappings tables.

    These tables store the symlink relationships for GCS mode.
    """
    stats = {"shared_devices": 0, "index_mappings": 0}

    if dry_run:
        print("\n[DRY-RUN] Would populate symlink tables:")
        for device in devices.values():
            owner = device.owner_client_id
            device_id = device.device_id

            # Shared devices (excluding owner)
            shared_count = len([c for c in device.shared_clients if c != owner])
            if shared_count > 0:
                print(f"  stream_shared_devices: {owner}/{device_id} -> {shared_count} shares")
                stats["shared_devices"] += shared_count

            # Secondary keys
            for client_id, keys in device.secondary_keys.items():
                print(f"  stream_index_mappings (secondary_key): {client_id} -> {len(keys)} keys")
                stats["index_mappings"] += len(keys)

            # Request IDs - commented out as per user's file modification
            # for client_id, req_ids in device.request_ids.items():
            #     print(f"  stream_index_mappings (request_id): {client_id} -> {len(req_ids)} ids")
            #     stats["index_mappings"] += len(req_ids)

        return stats

    conn = await asyncpg.connect(database_url)
    try:
        for device in devices.values():
            owner = device.owner_client_id
            device_id = device.device_id

            # Insert shared device records (for non-owner clients)
            for shared_client in device.shared_clients:
                if shared_client != owner:
                    await conn.execute(
                        """
                        INSERT INTO stream_shared_devices (owner_client_id, shared_client_id, device_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (owner_client_id, shared_client_id, device_id) DO NOTHING
                        """,
                        owner,
                        shared_client,
                        device_id,
                    )
                    stats["shared_devices"] += 1
                    print(f"  Created shared_device: {owner}/{device_id} -> {shared_client}")

            # Insert secondary_key mappings
            for client_id, keys in device.secondary_keys.items():
                for key in keys:
                    await conn.execute(
                        """
                        INSERT INTO stream_index_mappings
                            (client_id, index_type, index_key, target_device_id)
                        VALUES ($1, 'secondary_key', $2, $3)
                        ON CONFLICT (client_id, index_type, index_key) DO NOTHING
                        """,
                        client_id,
                        key,
                        device_id,
                    )
                    stats["index_mappings"] += 1
                    print(f"  Created secondary_key mapping: {client_id}/{key} -> {device_id}")

            # Request IDs - commented out as per user's file modification
            # for client_id, req_ids in device.request_ids.items():
            #     for req_id in req_ids:
            #         await conn.execute(
            #             """
            #             INSERT INTO stream_index_mappings
            #                 (client_id, index_type, index_key, target_device_id)
            #             VALUES ($1, 'request_id', $2, $3)
            #             ON CONFLICT (client_id, index_type, index_key) DO NOTHING
            #             """,
            #             client_id,
            #             req_id,
            #             device_id,
            #         )
            #         stats["index_mappings"] += 1
            #         print(f"  Created request_id mapping: {client_id}/{req_id} -> {device_id}")

    finally:
        await conn.close()

    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Migrate local storage to GCS with symlink support"
    )
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

    # Discover devices (handles symlinks)
    print("\n=== Discovering devices (with symlink resolution) ===")
    devices = discover_devices(args.source)

    if not devices:
        print("No devices with data found")
        return 0

    for device in devices.values():
        print(f"\nDevice: {device.owner_client_id}/{device.device_id}")
        print(f"  Owner: {device.owner_client_id}")
        print(f"  Frames: {device.frame_count}")
        print(f"  Segments: {device.segment_count}")
        print(f"  Shared with clients: {device.shared_clients}")

        if device.secondary_keys:
            for client_id, keys in device.secondary_keys.items():
                print(f"  Secondary keys ({client_id}): {len(keys)} keys")

        if device.request_ids:
            for client_id, req_ids in device.request_ids.items():
                print(f"  Request IDs ({client_id}): {len(req_ids)} requests")

        segment_info = analyze_segments(device.segments_path)
        if segment_info:
            print(
                f"  Segment range: {segment_info['first_segment']} - {segment_info['last_segment']}"
            )
            print(f"  Sessions: {len(segment_info.get('sessions', []))}")

        frame_info = analyze_frames(device.frames_path)
        if frame_info:
            print(f"  Time range: {frame_info['earliest']} to {frame_info['latest']}")
            print(f"  Duration: {frame_info['duration_seconds']:.0f}s")

    # Upload to GCS (only from owner directories)
    if not args.skip_upload:
        print("\n=== Uploading to GCS (owner directories only) ===")
        upload_stats = await upload_to_gcs(devices, bucket_name, args.dry_run)
        print(f"\nUpload stats: {upload_stats}")

    # Populate symlink tables (for GCS mode)
    print("\n=== Populating symlink tables ===")
    symlink_stats = await populate_symlink_tables(
        devices,
        database_url or "",
        args.dry_run,
    )
    print(f"\nSymlink table stats: {symlink_stats}")

    # Create deferred transmissions (for all clients)
    print("\n=== Creating deferred transmissions (for all shared clients) ===")
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
