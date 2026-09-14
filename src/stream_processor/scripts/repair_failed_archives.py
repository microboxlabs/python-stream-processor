#!/usr/bin/env python3
"""
Report on, repair, or discard archives left in 'failed' state.

`_archive_session` writes the segments and the VOD playlist before it writes
the database row, so a failure in that last step leaves a complete, playable
archive marked 'failed'. `cleanup_expired_archives` keeps those rows instead of
reaping them, which means nothing deletes them and nothing serves them. This
script settles them by hand.

    report   (default)  what each 'failed' archive holds. Read-only.
    --repair            mark the complete ones 'ready' and restart the clock.
    --discard           delete the storage and mark the rows 'deleted'.

Both mutating modes need --yes. Without it they report what they would do.

`--repair` only touches archives whose playlist exists, the same test the
reaper uses. `--discard` refuses an archive with a playlist unless you pass
--force, so a complete recording is never thrown away by a stray flag.

Usage (local, via SQL tunnel + gcloud ADC):

    export ARCHIVE_DATABASE_URL='postgresql://streamhub:PASSWORD@127.0.0.1:8432/prod_iot_gps'
    export STORAGE_TYPE=gcs
    export STORAGE_GCS_BUCKET=stream-frame
    uv run python -m stream_processor.scripts.repair_failed_archives

Or in the cluster, where both are already configured:

    kubectl exec -it deploy/prod-streamhub-stream-processor -- \\
        uv run python -m stream_processor.scripts.repair_failed_archives --repair --yes

Flags:
    --repair            Mark complete archives 'ready', expiring --extend-days
                        from now.
    --discard           Delete the storage and mark the rows 'deleted'.
    --yes               Apply the change. Without it, both modes are a dry run.
    --force             Let --discard delete an archive that has a playlist.
    --session-id UUID   Limit to one archive. Repeatable.
    --extend-days N     Retention a repaired archive gets (default: the
                        configured archive retention).
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from ..config.settings import settings
from ..service.storage_backend import StorageBackend, create_storage_backend
from ..utils.logger import get_logger

logger = get_logger(__name__)

SELECT_FAILED = """
    SELECT id, client_id, device_id, session_id, archive_path, segment_count,
           started_at, ended_at, duration_seconds, expires_at
    FROM deferred_transmissions
    WHERE status = 'failed'
    ORDER BY expires_at
    """

SELECT_FAILED_BY_SESSION = """
    SELECT id, client_id, device_id, session_id, archive_path, segment_count,
           started_at, ended_at, duration_seconds, expires_at
    FROM deferred_transmissions
    WHERE status = 'failed' AND session_id = ANY($1)
    ORDER BY expires_at
    """


class Archive:
    """A 'failed' row paired with what its storage actually holds."""

    def __init__(self, row, storage: StorageBackend):
        self.row = row
        self.has_playlist = storage.file_exists(
            row["client_id"], row["device_id"], f"{row['archive_path']}/playlist.m3u8"
        )
        self.segments = list(
            storage.list_files(
                row["client_id"],
                row["device_id"],
                f"{row['archive_path']}/segments",
                pattern="*.ts",
            )
        )

    @property
    def bytes(self) -> int:
        return sum(f.size for f in self.segments)

    @property
    def complete(self) -> bool:
        """The reaper's own test: the playlist is written last."""
        return self.has_playlist

    @property
    def missing(self) -> int:
        """
        Segments the row counted that are not in storage.

        segment_count usually undercounts — most archives hold a few more files
        than the row claims — so only a shortfall means anything, and it means
        the recording has gaps even though the playlist says it finished.
        """
        # asyncpg Records are Any, so type the local to keep the return typed.
        claimed: int = self.row["segment_count"]
        return max(0, claimed - len(self.segments))

    def describe(self) -> str:
        row = self.row
        state = "COMPLETE" if self.complete else "partial"
        counted = len(self.segments)
        claimed = row["segment_count"]
        drift = "" if counted == claimed else f" (row says {claimed})"
        if self.missing:
            drift += f", {self.missing} MISSING"
        # Not every failed row is expired, so report the date, not a verdict.
        return (
            f"{state:<8} id={row['id']:<6} session={row['session_id']} "
            f"device={row['device_id']} {counted} segments{drift} "
            f"{self.bytes / 1e9:.2f} GB {row['duration_seconds']}s "
            f"expires {row['expires_at']:%Y-%m-%d}"
        )


def build_storage() -> StorageBackend:
    cfg = settings.storage
    return create_storage_backend(
        storage_type=cfg.type,
        base_path=cfg.base_path,
        gcs_bucket=cfg.gcs_bucket,
        gcs_project_id=cfg.gcs_project_id,
    )


async def repair(pool, archive: Archive, extend_days: int) -> bool:
    """
    Mark the row 'ready' and restart its retention clock from now.

    Returns whether the row was still 'failed' and so actually changed.
    """
    expires_at = datetime.now(timezone.utc) + timedelta(days=extend_days)  # noqa: UP017
    claimed = await pool.fetchval(
        """
        UPDATE deferred_transmissions
        SET status = 'ready', expires_at = $2, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status = 'failed'
        RETURNING id
        """,
        archive.row["id"],
        expires_at,
    )
    return claimed is not None


async def discard(pool, storage: StorageBackend, archive: Archive) -> bool:
    """
    Mark the row 'deleted', then delete the archive's storage.

    The row is claimed first, and the files are deleted only if that claim
    won. Deleting the files first would let a --repair running in another
    shell flip the row to 'ready' in between, leaving a live row pointing at
    storage this run had already emptied. Claiming first trades that for a
    smaller risk: a crash mid-delete leaves orphaned files behind a 'deleted'
    row, which wastes space but loses nothing.

    Returns whether the row was still 'failed' and so actually changed.
    """
    row = archive.row
    claimed = await pool.fetchval(
        """
        UPDATE deferred_transmissions
        SET status = 'deleted', updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status = 'failed'
        RETURNING id
        """,
        row["id"],
    )
    if claimed is None:
        return False

    for file_info in archive.segments:
        storage.delete_file(
            row["client_id"],
            row["device_id"],
            f"{row['archive_path']}/segments/{file_info.name}",
        )
    storage.delete_file(row["client_id"], row["device_id"], f"{row['archive_path']}/playlist.m3u8")
    return True


async def fetch_failed(pool, session_ids: list[str] | None) -> list[Any]:
    """Fetch the 'failed' rows, narrowed to session_ids when given."""
    # asyncpg is untyped, so bind to a typed local rather than return Any.
    rows: list[Any]
    if session_ids:
        rows = await pool.fetch(SELECT_FAILED_BY_SESSION, session_ids)
    else:
        rows = await pool.fetch(SELECT_FAILED)
    return rows


def log_nothing_found(session_ids: list[str] | None) -> None:
    """Say why the result was empty, which is not always "nothing is broken"."""
    if session_ids:
        logger.info(
            f"No 'failed' archive matches {', '.join(session_ids)}. "
            f"Run without --session-id to list what is failed."
        )
    else:
        logger.info("No archives in 'failed' state")


def log_report(archives: list[Archive], complete: list[Archive]) -> None:
    """Log one line per archive, then warn about the ones with gaps."""
    partial_count = len(archives) - len(complete)
    logger.info(
        f"{len(archives)} failed archive(s): {len(complete)} complete, "
        f"{partial_count} partial, {sum(a.bytes for a in archives) / 1e9:.2f} GB total"
    )
    for a in archives:
        logger.info(a.describe())

    # Only the complete ones; --repair never touches the rest, so promising to
    # restore a partial archive here would contradict the skip message it gets.
    for a in complete:
        if a.missing:
            logger.warning(
                f"Archive {a.row['session_id']} is short {a.missing} segment(s). "
                f"Its playlist exists, so --repair restores it, but the "
                f"recording has gaps."
            )


def plan_action(args, archives, complete, partial) -> tuple[list[Archive], list[Archive], str, str]:
    """Split the archives into what this mode acts on and what it leaves."""
    if args.repair:
        return complete, partial, "repair", "no playlist, so the recording is incomplete"
    if args.force:
        return archives, [], "discard", ""
    return partial, complete, "discard", "playlist present; pass --force to discard anyway"


async def apply_action(pool, storage, args, targets: list[Archive]) -> int:
    """Run the chosen mutation over the targets, counting what really changed."""
    acted = 0
    for a in targets:
        if args.repair:
            changed = await repair(pool, a, args.extend_days)
            done = "now 'ready'"
        else:
            changed = await discard(pool, storage, a)
            done = "storage deleted"

        if changed:
            acted += 1
            logger.info(
                f"{'Repaired' if args.repair else 'Discarded'} {a.row['session_id']}: {done}"
            )
        else:
            logger.warning(
                f"Left {a.row['session_id']} alone: it stopped being 'failed' "
                f"after this run listed it. Re-run to see its current state."
            )
    return acted


async def run(args) -> int:
    database_url = settings.archive.database_url
    if not database_url:
        logger.error("ARCHIVE_DATABASE_URL must be set")
        return 2

    storage = build_storage()
    pool = await asyncpg.create_pool(database_url)
    try:
        rows = await fetch_failed(pool, args.session_id)
        if not rows:
            log_nothing_found(args.session_id)
            return 0

        archives = [Archive(r, storage) for r in rows]
        complete = [a for a in archives if a.complete]
        partial = [a for a in archives if not a.complete]
        log_report(archives, complete)

        if not (args.repair or args.discard):
            logger.info("Read-only. --repair restores the complete ones, --discard deletes")
            return 0

        targets, skipped, verb, why = plan_action(args, archives, complete, partial)
        for a in skipped:
            logger.info(f"Skipping {a.row['session_id']}: {why}")

        if not targets:
            logger.info(f"Nothing to {verb}")
            return 0

        if not args.yes:
            logger.info(f"Would {verb} {len(targets)} archive(s). Re-run with --yes")
            return 0

        acted = await apply_action(pool, storage, args, targets)
        if args.repair:
            logger.info(
                f"{acted} archive(s) marked 'ready', expiring in "
                f"{args.extend_days} days. Normal retention reaps them after that."
            )
        else:
            logger.info(f"{acted} archive(s) discarded")
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Report on, repair, or discard archives left in 'failed' state"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--repair", action="store_true", help="mark complete archives 'ready'")
    mode.add_argument("--discard", action="store_true", help="delete storage and mark 'deleted'")
    parser.add_argument(
        "--yes", action="store_true", help="apply the change; without it, a dry run"
    )
    parser.add_argument(
        "--force", action="store_true", help="let --discard delete an archive that has a playlist"
    )
    parser.add_argument(
        "--session-id", action="append", help="limit to this session id (repeatable)"
    )
    parser.add_argument(
        "--extend-days",
        type=int,
        default=settings.archive.retention_days,
        help="days of retention a repaired archive gets from now",
    )
    args = parser.parse_args()

    if args.force and not args.discard:
        parser.error("--force only applies to --discard")
    if args.extend_days <= 0:
        parser.error("--extend-days must be greater than zero")

    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
