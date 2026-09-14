"""
Minimal in-memory stand-in for the google-cloud-storage client.

Models the two things the cost work cares about:

- `list_blobs(delimiter="/")` returns only the objects directly under the
  prefix plus the collapsed sub-prefixes, and `prefixes` is populated as pages
  are walked (the real iterator behaves the same way).
- Every page fetched is one Class A operation and every blob-level request is
  one Class B operation, both counted so tests can assert on them.
"""

from google.api_core.exceptions import NotFound


class FakeBlob:
    """A single object in the fake bucket."""

    def __init__(self, bucket: "FakeBucket", name: str):
        self._bucket = bucket
        self.name = name

    @property
    def _data(self) -> bytes | None:
        return self._bucket.objects.get(self.name)

    @property
    def size(self) -> int:
        return len(self._data or b"")

    @property
    def updated(self):
        return self._bucket.mtimes.get(self.name)

    def exists(self) -> bool:
        self._bucket.class_b_ops += 1
        return self.name in self._bucket.objects

    def reload(self) -> None:
        self._bucket.class_b_ops += 1
        if self.name not in self._bucket.objects:
            raise NotFound(self.name)

    def download_as_bytes(self) -> bytes:
        self._bucket.class_b_ops += 1
        data = self._data
        if data is None:
            raise NotFound(self.name)
        return data

    def upload_from_string(self, data: bytes, content_type: str | None = None) -> None:
        self._bucket.objects[self.name] = data

    def delete(self) -> None:
        if self.name not in self._bucket.objects:
            raise NotFound(self.name)
        del self._bucket.objects[self.name]


class FakeBucket:
    """Holds the object map and the operation counters."""

    def __init__(self, name: str, objects: dict[str, bytes] | None = None):
        self.name = name
        self.objects: dict[str, bytes] = dict(objects or {})
        self.mtimes: dict = {}
        self.class_a_ops = 0  # list pages fetched
        self.class_b_ops = 0  # metadata reads / downloads

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)

    def get_blob(self, name: str) -> FakeBlob | None:
        self.class_b_ops += 1
        if name not in self.objects:
            return None
        return FakeBlob(self, name)


class FakeListIterator:
    """Paginated listing result; `prefixes` fills in as pages are consumed."""

    def __init__(self, bucket: FakeBucket, names: list[str], prefixes: list[str], page_size: int):
        self._bucket = bucket
        # GCS counts objects and collapsed prefixes together toward the page
        # size, and accumulates prefixes across pages. A listing that returns
        # more than page_size prefixes therefore costs more than one Class A op.
        self._items = [("name", n) for n in names] + [("prefix", p) for p in prefixes]
        self._page_size = page_size
        self.prefixes: set[str] = set()

    @property
    def pages(self):
        chunks = [
            self._items[i : i + self._page_size]
            for i in range(0, len(self._items), self._page_size)
        ] or [[]]
        for chunk in chunks:
            self._bucket.class_a_ops += 1
            self.prefixes.update(value for kind, value in chunk if kind == "prefix")
            yield [FakeBlob(self._bucket, value) for kind, value in chunk if kind == "name"]

    def __iter__(self):
        for page in self.pages:
            yield from page


class FakeGcsClient:
    """Stands in for `google.cloud.storage.Client`."""

    def __init__(self, bucket: FakeBucket, page_size: int = 1000):
        self._bucket = bucket
        self.page_size = page_size

    def bucket(self, name: str) -> FakeBucket:
        return self._bucket

    def list_blobs(self, bucket_name, prefix="", delimiter=None, **kwargs) -> FakeListIterator:
        matched = sorted(n for n in self._bucket.objects if n.startswith(prefix))

        if delimiter is None:
            return FakeListIterator(self._bucket, matched, [], self.page_size)

        names: list[str] = []
        prefixes: set[str] = set()
        for name in matched:
            remainder = name[len(prefix) :]
            head, sep, _ = remainder.partition(delimiter)
            if sep:
                prefixes.add(prefix + head + delimiter)
            else:
                names.append(name)

        return FakeListIterator(self._bucket, names, sorted(prefixes), self.page_size)
