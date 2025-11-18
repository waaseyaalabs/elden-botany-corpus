from __future__ import annotations

from pathlib import Path

import pytest
import requests

from corpus.config import settings
from corpus.ingest_carian_fmg import (
    CARIAN_FMG_RELATIVE_PATHS,
    fetch_carian_fmg_files,
)


class _FakeResponse:
    def __init__(self, payload: bytes, status_code: int = 200) -> None:
        self.content = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def test_fetch_carian_fmg_files_downloads_and_caches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    monkeypatch.setattr(settings, "raw_dir", raw_dir)

    requested_urls: list[str] = []

    def _fake_get(url: str, timeout: int) -> _FakeResponse:
        requested_urls.append(url)
        payload = f"<root><text>{Path(url).name}</text></root>".encode("utf-8")
        return _FakeResponse(payload)

    monkeypatch.setattr("corpus.ingest_carian_fmg.requests.get", _fake_get)

    downloaded = fetch_carian_fmg_files(force=True)
    assert len(downloaded) == len(CARIAN_FMG_RELATIVE_PATHS)
    assert len(requested_urls) == len(CARIAN_FMG_RELATIVE_PATHS)

    for expected, actual in zip(
        CARIAN_FMG_RELATIVE_PATHS,
        downloaded,
        strict=True,
    ):
        assert actual.exists()
        assert actual.relative_to(raw_dir / "carian_archive") == Path(expected)

    requested_urls.clear()
    cached = fetch_carian_fmg_files()
    assert cached == downloaded
    assert requested_urls == []

    forced = fetch_carian_fmg_files(force=True)
    assert forced == downloaded
    assert len(requested_urls) == len(CARIAN_FMG_RELATIVE_PATHS)
