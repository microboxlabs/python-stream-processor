"""
Repair VOD playlist EXTINF values for historic archives.

Older archives were written with a constant per-segment EXTINF based on
processing.segment_duration_seconds, which does not match the actual TS
content length — segments that claim 30 s of video only contain ~7 s of
timelapse frames, so HLS.js shrinks MediaSource duration to the real PTS
total and the player cannot scrub past the first ~minute.

This script reprobes each segment referenced by an archive's playlist and
rewrites playlist.m3u8 with accurate EXTINF values, reusing the same
helpers (`ArchiveService._probe_segment_duration` and
`ArchiveService._generate_vod_playlist`) that new archives already use, so
repaired and freshly-generated playlists are byte-identical.

Usage (local, via SQL tunnel + gcloud ADC):

    export ARCHIVE_DATABASE_URL='postgresql://streamhub:PASSWORD@127.0.0.1:8432/prod_iot_gps'
    export STORAGE_TYPE=gcs
    export STORAGE_GCS_BUCKET=stream-frame
    uv run python -m stream_processor.scripts.repair_archive_playlist \\
        --session-id a47ddc0d-4b4a-49d5-bd9d-8b2c9c7de331

Flags:
    --session-id UUID   Repair a single archive. Omit to process all
                        `status='ready'` archives in descending ended_at order.
    --limit N           When processing in bulk, stop after N archives.
"""

import argparse
import asyncio
import math
import sys

import asyncpg

from ..config.settings import settings
from ..service.archive_service import ArchiveService
from ..utils.logger import get_logger

logger = get_logger(__name__)


def _build_playlist_with_discontinuities(
    probed: list[tuple[str, float]], fallback_duration: float
) -> str:
    """
    Build a VOD playlist that emits #EXT-X-DISCONTINUITY between every
    segment, signalling to HLS.js that each segment carries fresh PTS
    starting from 0 (which is what the per-invocation FFmpeg pipeline
    actually produces). Without these tags the player's seek path can't
    realign timestampOffset and the buffer never extends to currentTime.
    """
    real_durations = [d for _, d in probed if d > 0]
    if real_durations:
        target_duration = math.ceil(max(real_durations))
    else:
        target_duration = math.ceil(fallback_duration)

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-DISCONTINUITY-SEQUENCE:0",
    ]
    for idx, (name, duration) in enumerate(sorted(probed, key=lambda x: x[0])):
        if idx > 0:
            lines.append("#EXT-X-DISCONTINUITY")
        extinf = duration if duration > 0 else fallback_duration
        lines.append(f"#EXTINF:{extinf:.3f},")
        lines.append(f"segments/{name}")
    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


def _parse_segment_names(playlist_text: str) -> list[str]:
    """Extract segment filenames from the URI lines of an .m3u8."""
    names: list[str] = []
    for raw in playlist_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Strip query string (token) and any path prefix.
        names.append(line.split("?", 1)[0].rsplit("/", 1)[-1])
    return names


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


async def _repair_one(
    service: ArchiveService,
    owner_client_id: str,
    device_id: str,
    archive_path: str,
    *,
    discontinuity: bool,
) -> tuple[int, int]:
    """
    Repair a single archive. Returns (probed_segments, probe_failures).

    Reads the existing playlist, parses out the segment filenames it
    references, reprobes each file, and rewrites the playlist in place.
    """
    playlist_bytes = service.storage.read_file(
        owner_client_id, device_id, f"{archive_path}/playlist.m3u8"
    )
    if playlist_bytes is None:
        raise FileNotFoundError(f"playlist.m3u8 not found for {archive_path}")

    segment_names = _parse_segment_names(playlist_bytes.decode("utf-8"))
    if not segment_names:
        raise ValueError(f"no segment entries in {archive_path}/playlist.m3u8")

    probed: list[tuple[str, float]] = []
    probe_failures = 0
    for name in segment_names:
        data = service.storage.read_file(
            owner_client_id, device_id, f"{archive_path}/segments/{name}"
        )
        if data is None:
            logger.warning(
                f"Segment missing, recording zero duration: {archive_path}/segments/{name}"
            )
            probe_failures += 1
            probed.append((name, 0.0))
            continue
        duration = service._probe_segment_duration(data)
        if duration <= 0:
            probe_failures += 1
        probed.append((name, duration))

    if discontinuity:
        # Each ffmpeg invocation produces segments with PTS restarting at 0.
        # Tell the player explicitly via #EXT-X-DISCONTINUITY so seeks can
        # realign timestampOffset.
        content = _build_playlist_with_discontinuities(
            probed, float(settings.processing.segment_duration_seconds)
        )
        service.storage.write_file_atomic(
            owner_client_id,
            device_id,
            f"{archive_path}/playlist.m3u8",
            content.encode("utf-8"),
            content_type="application/vnd.apple.mpegurl",
        )
    else:
        await service._generate_vod_playlist(owner_client_id, device_id, archive_path, probed)
    return len(probed), probe_failures


async def _run(args: argparse.Namespace) -> int:
    if not settings.archive.database_url:
        logger.error("ARCHIVE_DATABASE_URL must be set")
        return 2
    if settings.storage.type != "gcs":
        logger.warning(
            f"STORAGE_TYPE is '{settings.storage.type}', not 'gcs' — "
            "are you sure you're pointing at production storage?"
        )

    pool = await asyncpg.create_pool(settings.archive.database_url, min_size=1, max_size=2)
    try:
        targets = await _fetch_targets(pool, args.session_id, args.device_id, args.limit)
    finally:
        await pool.close()

    if not targets:
        logger.warning("No target archives found")
        return 0

    logger.info(f"Repairing {len(targets)} archive(s)")
    service = ArchiveService()

    ok = 0
    failed = 0
    for row in targets:
        session_id = row["session_id"]
        try:
            n_segments, probe_failures = await _repair_one(
                service,
                row["owner_client_id"],
                row["device_id"],
                row["archive_path"],
                discontinuity=args.discontinuity,
            )
            logger.info(
                f"Repaired {session_id}: {n_segments} segments ({probe_failures} probe failures)"
            )
            ok += 1
        except Exception as e:
            logger.error(f"Failed to repair {session_id}: {e}", exc_info=True)
            failed += 1

    logger.info(f"Done: {ok} repaired, {failed} failed out of {len(targets)}")
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair historic archive playlists with probed EXTINF values"
    )
    parser.add_argument(
        "--session-id",
        help="Repair a single archive by session UUID (default: all status='ready')",
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
        "--no-discontinuity",
        dest="discontinuity",
        action="store_false",
        default=True,
        help="Disable #EXT-X-DISCONTINUITY between segments (use only if the "
        "encoder produces continuous PTS across segments)",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
