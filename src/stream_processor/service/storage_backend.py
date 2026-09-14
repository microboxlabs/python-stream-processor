"""
Storage Backend Abstraction

Provides pluggable storage backends for filesystem and GCS operations.
Mirrors the Java implementation for consistency.
"""

import re
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..utils.logger import get_logger

logger = get_logger(__name__)


def sanitize_path_component(value: str | None) -> str:
    """Sanitize a path component by replacing non-alphanumeric chars with underscores."""
    if not value:
        return "unknown"
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


@dataclass
class FileInfo:
    """Information about a file in storage."""

    name: str
    size: int
    mtime: float  # Modification time as Unix timestamp


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    Supports both filesystem and cloud storage implementations.
    Path structure: client_ids/{client_id}/device_id/{device_id}/{subpath}
    """

    @abstractmethod
    def get_storage_type(self) -> str:
        """Return the storage type identifier (e.g., 'filesystem', 'gcs')."""
        pass

    @abstractmethod
    def ensure_directory_exists(self, client_id: str, device_id: str, subpath: str) -> None:
        """Ensure a directory exists for the given path components."""
        pass

    @abstractmethod
    def write_file(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """
        Write data to a file.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            subpath: Path relative to device directory (e.g., 'hls/segments/seg_000001.ts')
            data: File contents
            content_type: Optional MIME type

        Returns:
            Full path/URI to the written file
        """
        pass

    @abstractmethod
    def write_file_atomic(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """
        Write data atomically (for playlists that need consistent reads).

        Args:
            client_id: Client identifier
            device_id: Device identifier
            subpath: Path relative to device directory
            data: File contents
            content_type: Optional MIME type

        Returns:
            Full path/URI to the written file
        """
        pass

    @abstractmethod
    def read_file(self, client_id: str, device_id: str, subpath: str) -> bytes | None:
        """
        Read file contents.

        Returns:
            File contents or None if file doesn't exist
        """
        pass

    @abstractmethod
    def file_exists(self, client_id: str, device_id: str, subpath: str) -> bool:
        """Check if a file exists."""
        pass

    @abstractmethod
    def delete_file(self, client_id: str, device_id: str, subpath: str) -> bool:
        """
        Delete a file.

        Returns:
            True if file was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
    def get_file_info(self, client_id: str, device_id: str, subpath: str) -> FileInfo | None:
        """
        Get file metadata.

        Returns:
            FileInfo or None if file doesn't exist
        """
        pass

    @abstractmethod
    def list_files(
        self, client_id: str, device_id: str, subpath: str, pattern: str | None = None
    ) -> Iterator[FileInfo]:
        """
        List files in a directory.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            subpath: Directory path relative to device (e.g., 'hls/segments')
            pattern: Optional glob pattern (e.g., 'seg_*.ts')

        Yields:
            FileInfo for each matching file
        """
        pass

    @abstractmethod
    def list_all_devices(self) -> Iterator[tuple[str, str]]:
        """
        List all client_id/device_id pairs in storage.

        Yields:
            Tuples of (client_id, device_id)
        """
        pass

    @abstractmethod
    def get_local_path(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """
        Get a local filesystem path for a file (for FFmpeg compatibility).

        For filesystem backend, returns the actual path.
        For GCS backend, may download to temp file or return None if not supported.

        Returns:
            Local Path object or None if not available locally
        """
        pass

    @abstractmethod
    def get_local_directory(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """
        Get a local filesystem directory path (for FFmpeg output).

        For filesystem backend, returns the actual directory and ensures it exists.
        For GCS backend, returns a temp directory that will need to be synced.

        Returns:
            Local Path object to directory or None if not supported
        """
        pass

    def cleanup_temp_files(self, max_age_seconds: int = 600) -> int:
        """
        Remove temporary files older than max_age_seconds.

        Returns:
            Number of files removed
        """
        return 0

    def _build_object_path(self, client_id: str, device_id: str, subpath: str) -> str:
        """Build the full object path from components."""
        safe_client = sanitize_path_component(client_id)
        safe_device = sanitize_path_component(device_id)
        return f"client_ids/{safe_client}/device_id/{safe_device}/{subpath}"


class FilesystemStorageBackend(StorageBackend):
    """
    Filesystem-based storage backend.

    Stores files directly on the local filesystem.
    """

    def __init__(self, base_path: str):
        """
        Initialize filesystem storage.

        Args:
            base_path: Base directory for all storage operations
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized filesystem storage at {self.base_path}")

    def get_storage_type(self) -> str:
        return "filesystem"

    def _get_full_path(self, client_id: str, device_id: str, subpath: str) -> Path:
        """Get the full filesystem path for a file."""
        object_path = self._build_object_path(client_id, device_id, subpath)
        return self.base_path / object_path

    def ensure_directory_exists(self, client_id: str, device_id: str, subpath: str) -> None:
        """Ensure directory exists."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        full_path.mkdir(parents=True, exist_ok=True)

    def write_file(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """Write file to filesystem."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        return str(full_path)

    def write_file_atomic(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """Write file atomically using temp file + rename."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to temp file then rename for atomic operation
        temp_path = full_path.with_suffix(".tmp")
        temp_path.write_bytes(data)
        temp_path.rename(full_path)
        return str(full_path)

    def read_file(self, client_id: str, device_id: str, subpath: str) -> bytes | None:
        """Read file from filesystem."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        if full_path.exists():
            return full_path.read_bytes()
        return None

    def file_exists(self, client_id: str, device_id: str, subpath: str) -> bool:
        """Check if file exists."""
        return self._get_full_path(client_id, device_id, subpath).exists()

    def delete_file(self, client_id: str, device_id: str, subpath: str) -> bool:
        """Delete file from filesystem."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        if full_path.exists():
            full_path.unlink()
            return True
        return False

    def get_file_info(self, client_id: str, device_id: str, subpath: str) -> FileInfo | None:
        """Get file metadata."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        if full_path.exists():
            stat = full_path.stat()
            return FileInfo(name=full_path.name, size=stat.st_size, mtime=stat.st_mtime)
        return None

    def list_files(
        self, client_id: str, device_id: str, subpath: str, pattern: str | None = None
    ) -> Iterator[FileInfo]:
        """List files in directory."""
        dir_path = self._get_full_path(client_id, device_id, subpath)
        if not dir_path.exists():
            return

        if pattern:
            files = dir_path.glob(pattern)
        else:
            files = dir_path.iterdir()

        for f in files:
            if f.is_file():
                stat = f.stat()
                yield FileInfo(name=f.name, size=stat.st_size, mtime=stat.st_mtime)

    def list_all_devices(self) -> Iterator[tuple[str, str]]:
        """List all client/device pairs."""
        client_ids_path = self.base_path / "client_ids"
        if not client_ids_path.exists():
            return

        for client_dir in client_ids_path.iterdir():
            if not client_dir.is_dir():
                continue

            device_id_dir = client_dir / "device_id"
            if not device_id_dir.exists():
                continue

            for device_dir in device_id_dir.iterdir():
                if device_dir.is_dir():
                    yield (client_dir.name, device_dir.name)

    def get_local_path(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """Get local path (always available for filesystem)."""
        full_path = self._get_full_path(client_id, device_id, subpath)
        if full_path.exists():
            return full_path
        return None

    def get_local_directory(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """Get local directory path and ensure it exists."""
        dir_path = self._get_full_path(client_id, device_id, subpath)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path


class GcsStorageBackend(StorageBackend):
    """
    Google Cloud Storage backend.

    Stores files in a GCS bucket with the same path structure as filesystem.
    Uses Application Default Credentials for authentication.
    """

    def __init__(self, bucket_name: str, project_id: str | None = None):
        """
        Initialize GCS storage.

        Args:
            bucket_name: GCS bucket name
            project_id: Optional GCP project ID (uses ADC if not specified)
        """
        self.bucket_name = bucket_name
        self.project_id = project_id

        # Initialize client lazily; google.cloud is imported on first use.
        # Any, not storage.Client: the library ships no type stubs.
        self._client: Any = None
        self._bucket: Any = None

        # Temp directory for local operations (FFmpeg compatibility)
        self._temp_dir = Path(tempfile.mkdtemp(prefix="stream_processor_gcs_"))

        logger.info(f"Initialized GCS storage with bucket: {bucket_name}")

    @property
    def client(self):
        """Lazy-load GCS client."""
        client = self._client
        if client is None:
            from google.cloud import storage

            if self.project_id:
                client = storage.Client(project=self.project_id)
            else:
                client = storage.Client()
            self._client = client
        return client

    @property
    def bucket(self):
        """Lazy-load bucket reference."""
        bucket = self._bucket
        if bucket is None:
            bucket = self.client.bucket(self.bucket_name)
            self._bucket = bucket
        return bucket

    def get_storage_type(self) -> str:
        return "gcs"

    def _get_blob_path(self, client_id: str, device_id: str, subpath: str) -> str:
        """Get the GCS blob path."""
        return self._build_object_path(client_id, device_id, subpath)

    def _get_content_type(self, filename: str) -> str:
        """Determine content type from filename."""
        lower_name = filename.lower()
        if lower_name.endswith(".ts"):
            return "video/mp2t"
        if lower_name.endswith(".m3u8"):
            return "application/vnd.apple.mpegurl"
        if lower_name.endswith((".jpg", ".jpeg")):
            return "image/jpeg"
        if lower_name.endswith(".png"):
            return "image/png"
        return "application/octet-stream"

    def ensure_directory_exists(self, client_id: str, device_id: str, subpath: str) -> None:
        """No-op for GCS (directories are virtual)."""
        pass

    def write_file(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """Write file to GCS."""
        blob_path = self._get_blob_path(client_id, device_id, subpath)
        blob = self.bucket.blob(blob_path)

        if content_type is None:
            content_type = self._get_content_type(subpath)

        blob.upload_from_string(data, content_type=content_type)
        uri = f"gs://{self.bucket_name}/{blob_path}"
        logger.debug(f"Wrote file to GCS: {uri}")
        return uri

    def write_file_atomic(
        self,
        client_id: str,
        device_id: str,
        subpath: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """Write file to GCS (GCS writes are already atomic)."""
        return self.write_file(client_id, device_id, subpath, data, content_type)

    def read_file(self, client_id: str, device_id: str, subpath: str) -> bytes | None:
        """Read file from GCS."""
        from google.api_core.exceptions import NotFound

        blob_path = self._get_blob_path(client_id, device_id, subpath)
        blob = self.bucket.blob(blob_path)

        # Download straight away: exists() would be a second billable Class B op.
        try:
            result: bytes = blob.download_as_bytes()
        except NotFound:
            return None
        return result

    def file_exists(self, client_id: str, device_id: str, subpath: str) -> bool:
        """Check if file exists in GCS."""
        blob_path = self._get_blob_path(client_id, device_id, subpath)
        blob = self.bucket.blob(blob_path)
        result: bool = blob.exists()
        return result

    def delete_file(self, client_id: str, device_id: str, subpath: str) -> bool:
        """Delete file from GCS."""
        from google.api_core.exceptions import NotFound

        blob_path = self._get_blob_path(client_id, device_id, subpath)
        blob = self.bucket.blob(blob_path)

        # delete() is free; exists() is a billable Class B op. Delete and catch.
        try:
            blob.delete()
        except NotFound:
            return False
        return True

    def get_file_info(self, client_id: str, device_id: str, subpath: str) -> FileInfo | None:
        """Get file metadata from GCS."""
        blob_path = self._get_blob_path(client_id, device_id, subpath)

        # get_blob() fetches metadata in one op and returns None when missing;
        # exists() + reload() would cost two.
        blob = self.bucket.get_blob(blob_path)
        if blob is None:
            return None

        return FileInfo(
            name=blob_path.split("/")[-1],
            size=blob.size or 0,
            mtime=blob.updated.timestamp() if blob.updated else 0,
        )

    def list_files(
        self, client_id: str, device_id: str, subpath: str, pattern: str | None = None
    ) -> Iterator[FileInfo]:
        """List files in GCS 'directory'."""
        prefix = self._get_blob_path(client_id, device_id, subpath)
        if not prefix.endswith("/"):
            prefix += "/"

        # Convert glob pattern to prefix matching
        # For simple patterns like 'seg_*.ts', we list all and filter
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter="/")

        for blob in blobs:
            filename = blob.name.split("/")[-1]

            # Apply pattern filter if specified
            if pattern:
                import fnmatch

                if not fnmatch.fnmatch(filename, pattern):
                    continue

            yield FileInfo(
                name=filename,
                size=blob.size or 0,
                mtime=blob.updated.timestamp() if blob.updated else 0,
            )

    def _list_prefixes(self, prefix: str) -> list[str]:
        """
        List the immediate "subdirectory" prefixes under a prefix.

        delimiter="/" makes GCS collapse everything below one level into a
        prefix. The iterator exposes prefixes per page, so every page has to be
        walked to collect them all.
        """
        iterator = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter="/")

        prefixes: set[str] = set()
        for _page in self._walk_pages(iterator):
            prefixes.update(iterator.prefixes)

        return sorted(prefixes)

    def _walk_pages(self, iterator):
        """
        Yield each page of a listing. One page is one Class A operation.

        A seam for tooling that meters scan cost; see scripts/gcs_scan_cost.py.
        """
        return iterator.pages

    def list_all_devices(self) -> Iterator[tuple[str, str]]:
        """
        List all client/device pairs in GCS.

        Walks two prefix levels (client_ids/{client_id}/device_id/{device_id}/)
        rather than enumerating every object under client_ids/. Costs one
        Class A operation per prefix page: 1 + N_clients while each level fits
        in a page, more once a level exceeds 1000 prefixes. Either way it does
        not depend on how many objects are stored.
        """
        for client_prefix in self._list_prefixes("client_ids/"):
            # client_prefix is "client_ids/{client_id}/"
            client_id = client_prefix.rstrip("/").split("/")[-1]
            if not client_id:
                continue

            for device_prefix in self._list_prefixes(f"{client_prefix}device_id/"):
                # device_prefix is "client_ids/{client_id}/device_id/{device_id}/"
                device_id = device_prefix.rstrip("/").split("/")[-1]
                if device_id:
                    yield (client_id, device_id)

    def get_local_path(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """
        Download file to temp directory and return local path.

        For FFmpeg to read input frames from GCS.
        """
        data = self.read_file(client_id, device_id, subpath)
        if data is None:
            return None

        # Create local path mirroring the structure
        local_path = self._temp_dir / self._build_object_path(client_id, device_id, subpath)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        return local_path

    def get_local_directory(self, client_id: str, device_id: str, subpath: str) -> Path | None:
        """
        Get a local temp directory for FFmpeg output.

        Files written here need to be synced back to GCS.
        """
        local_dir = self._temp_dir / self._build_object_path(client_id, device_id, subpath)
        local_dir.mkdir(parents=True, exist_ok=True)
        return local_dir

    def sync_local_to_gcs(self, client_id: str, device_id: str, subpath: str, filename: str) -> str:
        """
        Sync a local temp file back to GCS.

        Call this after FFmpeg writes output to the temp directory.

        Args:
            client_id: Client identifier
            device_id: Device identifier
            subpath: Directory subpath (e.g., 'hls/segments')
            filename: Name of file in the local directory

        Returns:
            GCS URI of the uploaded file
        """
        local_dir = self._temp_dir / self._build_object_path(client_id, device_id, subpath)
        local_file = local_dir / filename

        if not local_file.exists():
            raise FileNotFoundError(f"Local file not found: {local_file}")

        data = local_file.read_bytes()
        full_subpath = f"{subpath}/{filename}" if subpath else filename
        uri = self.write_file(client_id, device_id, full_subpath, data)

        # Clean up local temp file
        local_file.unlink()

        return uri

    def cleanup_temp(self) -> None:
        """Clean up all temporary files."""
        import shutil

        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def cleanup_temp_files(self, max_age_seconds: int = 600) -> int:
        """
        Remove temporary files older than max_age_seconds and prune empty directories.

        Returns:
            Number of files removed
        """
        import time

        if not self._temp_dir.exists():
            return 0

        now = time.time()
        cutoff = now - max_age_seconds
        removed = 0

        # Remove old files
        for file_path in self._temp_dir.rglob("*"):
            if not file_path.is_file():
                continue
            try:
                if file_path.stat().st_mtime < cutoff:
                    file_path.unlink()
                    removed += 1
            except OSError:
                pass

        # Prune empty directories (bottom-up)
        for dir_path in sorted(self._temp_dir.rglob("*"), reverse=True):
            if not dir_path.is_dir():
                continue
            try:
                dir_path.rmdir()  # only removes if empty
            except OSError:
                pass

        return removed

    def download_gcs_uri_to_local(self, gcs_uri: str) -> Path | None:
        """
        Download a file from a GCS URI to a local temp path.

        Args:
            gcs_uri: GCS URI in format gs://bucket/path/to/file

        Returns:
            Local Path to downloaded file, or None if download failed
        """
        if not gcs_uri.startswith("gs://"):
            logger.warning(f"Invalid GCS URI: {gcs_uri}")
            return None

        try:
            # Parse URI: gs://bucket/path/to/file
            uri_parts = gcs_uri[5:].split("/", 1)
            if len(uri_parts) != 2:
                logger.warning(f"Invalid GCS URI format: {gcs_uri}")
                return None

            bucket_name = uri_parts[0]
            blob_path = uri_parts[1]

            # Get the bucket (may be different from self.bucket_name)
            if bucket_name == self.bucket_name:
                bucket = self.bucket
            else:
                bucket = self.client.bucket(bucket_name)

            blob = bucket.blob(blob_path)

            if not blob.exists():
                logger.debug(f"GCS file not found: {gcs_uri}")
                return None

            # Create local path preserving structure
            local_path = self._temp_dir / blob_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download to local file
            blob.download_to_filename(str(local_path))
            logger.debug(f"Downloaded GCS file to: {local_path}")

            return local_path

        except Exception as e:
            logger.error(f"Failed to download GCS URI {gcs_uri}: {e}")
            return None


def download_gcs_uri(gcs_uri: str, temp_dir: Path | None = None) -> Path | None:
    """
    Standalone function to download a file from a GCS URI.

    This is useful when frames come from GCS but we don't have a
    GcsStorageBackend instance (e.g., filesystem mode receiving GCS URIs).

    Args:
        gcs_uri: GCS URI in format gs://bucket/path/to/file
        temp_dir: Optional temp directory to use (creates one if not provided)

    Returns:
        Local Path to downloaded file, or None if download failed
    """
    if not gcs_uri.startswith("gs://"):
        logger.warning(f"Invalid GCS URI: {gcs_uri}")
        return None

    try:
        from google.cloud import storage

        # Parse URI: gs://bucket/path/to/file
        uri_parts = gcs_uri[5:].split("/", 1)
        if len(uri_parts) != 2:
            logger.warning(f"Invalid GCS URI format: {gcs_uri}")
            return None

        bucket_name = uri_parts[0]
        blob_path = uri_parts[1]

        # Create temp directory if needed
        if temp_dir is None:
            temp_dir = Path(tempfile.mkdtemp(prefix="gcs_download_"))

        # Get the blob
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            logger.debug(f"GCS file not found: {gcs_uri}")
            return None

        # Create local path preserving structure
        local_path = temp_dir / blob_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download to local file
        blob.download_to_filename(str(local_path))
        logger.debug(f"Downloaded GCS file to: {local_path}")

        return local_path

    except Exception as e:
        logger.error(f"Failed to download GCS URI {gcs_uri}: {e}")
        return None


def create_storage_backend(
    storage_type: str,
    base_path: str | None = None,
    gcs_bucket: str | None = None,
    gcs_project_id: str | None = None,
) -> StorageBackend:
    """
    Factory function to create the appropriate storage backend.

    Args:
        storage_type: 'filesystem' or 'gcs'
        base_path: Base path for filesystem storage
        gcs_bucket: GCS bucket name (required for gcs type)
        gcs_project_id: Optional GCS project ID

    Returns:
        Configured StorageBackend instance
    """
    storage_type = storage_type.lower()

    if storage_type in ("filesystem", "local"):
        if not base_path:
            base_path = "/storage/streams"
        return FilesystemStorageBackend(base_path)

    elif storage_type in ("gcs", "google", "cloud"):
        if not gcs_bucket:
            raise ValueError("GCS bucket name is required when storage_type is 'gcs'")
        return GcsStorageBackend(gcs_bucket, gcs_project_id)

    else:
        raise ValueError(f"Unknown storage type: {storage_type}. Use 'filesystem' or 'gcs'")
