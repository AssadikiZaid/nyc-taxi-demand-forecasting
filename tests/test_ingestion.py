from pathlib import Path

import pytest

from taxi_forecast.config import load_config
from taxi_forecast.ingestion.tlc import FileNotPublishedError, download_file, download_months
from taxi_forecast.paths import raw_trip_file, zone_lookup_file

URL = "https://example.test/yellow_tripdata_2025-01.parquet"


class FakeResponse:
    def __init__(self, status_code=200, content=b"", headers=None):
        self.status_code = status_code
        self._content = content
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size):
        for start in range(0, len(self._content), chunk_size):
            yield self._content[start : start + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    """Serves a fixed set of files; ``truncate`` simulates a connection cut mid-download."""

    def __init__(self, files: dict[str, bytes], truncate: bool = False):
        self.files = files
        self.truncate = truncate
        self.gets: list[str] = []

    def head(self, url, timeout, allow_redirects):
        if url not in self.files:
            return FakeResponse(403)
        return FakeResponse(headers={"Content-Length": str(len(self.files[url]))})

    def get(self, url, stream, timeout):
        self.gets.append(url)
        if url not in self.files:
            return FakeResponse(403)
        content = self.files[url]
        return FakeResponse(content=content[: len(content) // 2] if self.truncate else content)


def test_downloads_a_missing_file(tmp_path: Path):
    session = FakeSession({URL: b"parquet-bytes"})
    destination = tmp_path / "raw" / "file.parquet"

    result = download_file(URL, destination, session)

    assert result.status == "downloaded"
    assert destination.read_bytes() == b"parquet-bytes"


def test_skips_a_file_already_downloaded(tmp_path: Path):
    session = FakeSession({URL: b"parquet-bytes"})
    destination = tmp_path / "file.parquet"
    destination.write_bytes(b"parquet-bytes")

    result = download_file(URL, destination, session)

    assert result.status == "skipped"
    assert session.gets == []


def test_replaces_a_local_file_of_the_wrong_size(tmp_path: Path):
    session = FakeSession({URL: b"complete-file"})
    destination = tmp_path / "file.parquet"
    destination.write_bytes(b"half")

    result = download_file(URL, destination, session)

    assert result.status == "downloaded"
    assert destination.read_bytes() == b"complete-file"


def test_unpublished_month_raises_a_clear_error(tmp_path: Path):
    with pytest.raises(FileNotPublishedError, match="not available"):
        download_file(URL, tmp_path / "file.parquet", FakeSession({}))


def test_interrupted_download_leaves_no_file_behind(tmp_path: Path):
    destination = tmp_path / "file.parquet"

    with pytest.raises(OSError, match="Incomplete download"):
        download_file(URL, destination, FakeSession({URL: b"0123456789"}, truncate=True))

    assert list(tmp_path.iterdir()) == []


def test_download_months_fetches_the_zone_table_and_each_month(tmp_path: Path):
    config = load_config()
    files = {config.data.zone_lookup_url: b"LocationID\n1\n"}
    for month in (1, 2):
        files[config.data.tlc_url(2025, month)] = f"month-{month}".encode()

    results = download_months([(2025, 1), (2025, 2)], config, tmp_path, FakeSession(files))

    assert [r.status for r in results] == ["downloaded"] * 3
    assert zone_lookup_file(tmp_path).exists()
    assert raw_trip_file(tmp_path, 2025, 2).read_bytes() == b"month-2"
