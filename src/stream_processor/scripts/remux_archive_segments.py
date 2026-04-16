"""
Re-mux archive segments so each one carries continuous PTS across the
whole archive.

The python-stream-processor pipeline writes each segment with a fresh
ffmpeg invocation, so every segment's MPEG-TS PTS restarts at 0. Sequential
playback works because HLS.js auto-adjusts timestampOffset segment-by-segment
as it appends, but seeking breaks: when HLS.js fetches the segment that
contains the seek target, the freshly-loaded fragment lands in the
SourceBuffer at PTS 0..N rather than at [seek_target..seek_target+N], so
the buffer never extends to currentTime and the player stalls forever.

Adding `#EXT-X-DISCONTINUITY` tags between segments doesn't help — putting
one between every segment forces a SourceBuffer flush and demuxer reset
per segment, which prevents playback from starting.

The honest fix is to remux every segment with `-output_ts_offset {cumulative}`
so its PTS picks up where the previous one ended. Then the timeline is
naturally continuous, no DISCONTINUITY tags are needed, and seeks land in
the right buffered range.

Usage (local, via SQL tunnel + GCS SA key):

    export ARCHIVE_DATABASE_URL='postgresql://streamhub:PASSWORD@127.0.0.1:8432/prod_iot_gps'
    export STORAGE_TYPE=gcs
    export STORAGE_GCS_BUCKET=stream-frame
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/.frame-sa.key.json
    uv run python -m stream_processor.scripts.remux_archive_segments \\
        --session-id 6e124769-6a0c-4688-a1e9-4e29552e1998

Idempotency: the script probes the LAST segment's first PTS. If it is
non-zero, the archive is treated as already remuxed and skipped. Use
`--force` to re-mux anyway (useful only if you've changed the offset
formula).
"""

import argparse
import asyncio
import math
import subprocess
import sys
import tempfile
from pathlib import Path

import asyncpg

from ..config.settings import settings
from ..service.archive_service import ArchiveService
from ..utils.logger import get_logger

logger = get_logger(__name__)


def _parse_segment_names(playlist_text: str) -> list[str]:
    names: list[str] = []
    for raw in playlist_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        names.append(line.split("?", 1)[0].rsplit("/", 1)[-1])
    return names


def _probe_first_pts(data: bytes) -> float:
    """Return the PTS (seconds) of the first packet, or 0.0 on failure."""
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".ts", prefix="probe_", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "packet=pts_time",
                "-of",
                "csv=p=0",
                str(tmp_path),
            ],
            capture_output=True,
            check=True,
            timeout=10,
        )
        first = result.stdout.decode().splitlines()[0].rstrip(",")
        return float(first)
    except (subprocess.SubprocessError, ValueError, IndexError):
        return 0.0
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _probe_duration(data: bytes) -> float:
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".ts", prefix="probe_", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nw=1:nk=1",
                str(tmp_path),
            ],
            capture_output=True,
            check=True,
            timeout=10,
        )
        return float(result.stdout.decode().strip())
    except (subprocess.SubprocessError, ValueError):
        return 0.0
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _remux_with_offset(data: bytes, offset_seconds: float) -> bytes:
    """
    Re-mux a TS segment, shifting its PTS by `offset_seconds`. The encoded
    video stream is copied bit-for-bit (`-c copy`), only container PTS is
    rewritten.
    """
    in_path: Path | None = None
    out_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".ts", prefix="remux_in_", delete=False) as tmp:
            tmp.write(data)
            in_path = Path(tmp.name)
        out_fd, out_name = tempfile.mkstemp(suffix=".ts", prefix="remux_out_")
        # Close the fd; ffmpeg will rewrite the file.
        import os

        os.close(out_fd)
        out_path = Path(out_name)
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(in_path),
                "-c",
                "copy",
                "-copyts",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                "-output_ts_offset",
                f"{offset_seconds:.6f}",
                "-f",
                "mpegts",
                str(out_path),
            ],
            capture_output=True,
            check=True,
            timeout=30,
        )
        return out_path.read_bytes()
    finally:
        for p in (in_path, out_path):
            if p is not None:
                try:
                    p.unlink()
                except OSError:
                    pass


def _build_continuous_playlist(probed: list[tuple[str, float]], fallback_duration: float) -> str:
    """Build a VOD playlist with no DISCONTINUITY (segments now continuous)."""
    real = [d for _, d in probed if d > 0]
    target = math.ceil(max(real)) if real else math.ceil(fallback_duration)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target}",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        "#EXT-X-MEDIA-SEQUENCE:0",
    ]
    for name, duration in sorted(probed, key=lambda x: x[0]):
        extinf = duration if duration > 0 else fallback_duration
        lines.append(f"#EXTINF:{extinf:.3f},")
        lines.append(f"segments/{name}")
    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


async def _fetch_targets(
    pool: asyncpg.Pool,
    session_id: str | None,
    device_id: str | None,
    limit: int | None,
) -> list[asyncpg.Record]:
    # Routing pool.fetch through a typed local satisfies mypy's no-any-return
    # check — asyncpg's stubs declare fetch() as returning Any.
    if session_id is not None:
        rows: list[asyncpg.Record] = await pool.fetch(
            """
            SELECT session_id, owner_client_id, device_id, archive_path
            FROM deferred_transmissions
            WHERE session_id = $1 AND status = 'ready'
            """,
            session_id,
        )
        return rows

    base = """
        SELECT session_id, owner_client_id, device_id, archive_path
        FROM deferred_transmissions
        WHERE status = 'ready'
    """
    params: list[object] = []
    if device_id is not None:
        params.append(device_id)
        base += f" AND device_id = ${len(params)}"
    base += " ORDER BY ended_at DESC"
    if limit is not None:
        params.append(limit)
        base += f" LIMIT ${len(params)}"
    rows = await pool.fetch(base, *params)
    return rows


async def _remux_one(
    service: ArchiveService,
    owner_client_id: str,
    device_id: str,
    archive_path: str,
    *,
    force: bool,
) -> tuple[int, int, bool]:
    """
    Returns (n_remuxed, n_skipped, already_done). already_done=True means
    the archive looked already remuxed and was skipped wholesale.
    """
    playlist_bytes = service.storage.read_file(
        owner_client_id, device_id, f"{archive_path}/playlist.m3u8"
    )
    if playlist_bytes is None:
        raise FileNotFoundError(f"playlist.m3u8 not found for {archive_path}")

    segment_names = sorted(_parse_segment_names(playlist_bytes.decode("utf-8")))
    if not segment_names:
        raise ValueError(f"no segments in {archive_path}")

    # Idempotency check: probe the last segment's first PTS. If it's non-zero,
    # the archive has already been remuxed.
    last_data = service.storage.read_file(
        owner_client_id, device_id, f"{archive_path}/segments/{segment_names[-1]}"
    )
    if last_data is None:
        raise FileNotFoundError(
            f"last segment missing: {archive_path}/segments/{segment_names[-1]}"
        )
    last_first_pts = _probe_first_pts(last_data)
    if last_first_pts > 0.5 and not force:
        logger.info(
            f"{archive_path}: last segment already has PTS offset "
            f"({last_first_pts:.3f}s); skipping. Use --force to re-mux anyway."
        )
        return 0, 0, True

    probed: list[tuple[str, float]] = []
    cumulative = 0.0
    n_remuxed = 0
    n_skipped = 0
    for name in segment_names:
        data = service.storage.read_file(
            owner_client_id, device_id, f"{archive_path}/segments/{name}"
        )
        if data is None:
            logger.warning(f"Segment missing, leaving placeholder duration: {name}")
            probed.append((name, 0.0))
            n_skipped += 1
            continue
        duration = _probe_duration(data)
        if duration <= 0:
            logger.warning(f"Probe failed for {name}; not re-muxing")
            probed.append((name, 0.0))
            n_skipped += 1
            continue
        new_data = _remux_with_offset(data, cumulative)
        service.storage.write_file(
            owner_client_id,
            device_id,
            f"{archive_path}/segments/{name}",
            new_data,
            content_type="video/mp2t",
        )
        probed.append((name, duration))
        cumulative += duration
        n_remuxed += 1

    # Rewrite playlist with the same EXTINFs (no DISCONTINUITY needed now
    # because segments share a continuous timeline).
    content = _build_continuous_playlist(
        probed, float(settings.processing.segment_duration_seconds)
    )
    service.storage.write_file_atomic(
        owner_client_id,
        device_id,
        f"{archive_path}/playlist.m3u8",
        content.encode("utf-8"),
        content_type="application/vnd.apple.mpegurl",
    )
    return n_remuxed, n_skipped, False


async def _run(args: argparse.Namespace) -> int:
    if not settings.archive.database_url:
        logger.error("ARCHIVE_DATABASE_URL must be set")
        return 2
    if settings.storage.type != "gcs":
        logger.warning(f"STORAGE_TYPE is '{settings.storage.type}', not 'gcs'")

    pool = await asyncpg.create_pool(settings.archive.database_url, min_size=1, max_size=2)
    try:
        targets = await _fetch_targets(pool, args.session_id, args.device_id, args.limit)
    finally:
        await pool.close()

    if not targets:
        logger.warning("No target archives found")
        return 0

    logger.info(f"Re-muxing {len(targets)} archive(s)")
    service = ArchiveService()

    ok = 0
    failed = 0
    skipped = 0
    for row in targets:
        session_id = row["session_id"]
        try:
            n_remuxed, n_failed, already = await _remux_one(
                service,
                row["owner_client_id"],
                row["device_id"],
                row["archive_path"],
                force=args.force,
            )
            if already:
                skipped += 1
            else:
                logger.info(
                    f"Re-muxed {session_id}: {n_remuxed} segments "
                    f"({n_failed} skipped due to probe failure)"
                )
                ok += 1
        except Exception as e:
            logger.error(f"Failed to re-mux {session_id}: {e}", exc_info=True)
            failed += 1

    logger.info(
        f"Done: {ok} re-muxed, {skipped} already-done, {failed} failed out of {len(targets)}"
    )
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-mux archive segments to have continuous PTS")
    parser.add_argument(
        "--session-id",
        help="Re-mux a single archive by session UUID (default: all status='ready')",
    )
    parser.add_argument(
        "--device-id",
        help="Restrict bulk mode to one sanitized device id "
        "(e.g. 30_dd_aa_03_11_0e). Ignored when --session-id is set.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="When processing in bulk, stop after N archives",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-mux even if the archive looks already remuxed",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
