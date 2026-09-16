"""Tests for download retries and verification.

Every scenario here was observed against the real source before these tests existed:
Wikimedia returned `429 Too Many Requests` when the pipeline opened more connections
than its 3-per-IP cap, and one transfer closed after 15 MB of a 52 MB file. The audit
table is how they were found.
"""

from __future__ import annotations

import gzip
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from wikiflow.download import (
    MAX_BACKOFF_SECONDS,
    DownloadVerificationError,
    download_dump,
)

URL = "https://dumps.wikimedia.org/other/pageviews/2026/2026-09/pageviews-20260914-120000.gz"
GOOD_BODY = gzip.compress(b"en Apple_Inc. 100 0\nde Apple 42 0\n")


def client_for(responses: list[httpx.Response | Exception]) -> tuple[httpx.Client, list[int]]:
    """A client that serves `responses` in order, counting requests."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        item = responses[min(len(calls), len(responses)) - 1]
        if isinstance(item, Exception):
            raise item
        return item

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def recorder() -> tuple[Callable[[float], None], list[float]]:
    slept: list[float] = []
    return slept.append, slept


@pytest.mark.unit
def test_plain_success_needs_one_request(tmp_path: Path) -> None:
    client, calls = client_for([httpx.Response(200, content=GOOD_BODY)])
    sleep, slept = recorder()

    result = download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert len(calls) == 1
    assert slept == []
    assert result.line_count == 2


@pytest.mark.unit
def test_429_is_retried_and_retry_after_is_honoured_exactly(tmp_path: Path) -> None:
    """The server says how long it needs. Waiting less is how clients get blocked."""
    client, calls = client_for(
        [
            httpx.Response(429, headers={"Retry-After": "37"}),
            httpx.Response(200, content=GOOD_BODY),
        ]
    )
    sleep, slept = recorder()

    result = download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert len(calls) == 2
    assert slept == [37.0]
    assert result.line_count == 2


@pytest.mark.unit
def test_without_retry_after_backoff_is_exponential(tmp_path: Path) -> None:
    client, _ = client_for(
        [
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, content=GOOD_BODY),
        ]
    )
    sleep, slept = recorder()

    download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert slept == [10.0, 20.0, 40.0]


@pytest.mark.unit
def test_404_is_not_retried(tmp_path: Path) -> None:
    """A missing file is not transient. Retrying it only adds load to the server."""
    client, calls = client_for([httpx.Response(404)])
    sleep, slept = recorder()

    with pytest.raises(httpx.HTTPStatusError):
        download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert len(calls) == 1
    assert slept == []


@pytest.mark.unit
def test_gives_up_after_max_attempts_with_the_real_error(tmp_path: Path) -> None:
    """The error that reaches the audit table must be the 429, not a wrapper."""
    client, calls = client_for([httpx.Response(429, headers={"Retry-After": "1"})])
    sleep, _ = recorder()

    with pytest.raises(httpx.HTTPStatusError, match="429") as raised:
        download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep, max_attempts=3)

    assert len(calls) == 3
    assert any("gave up after 3 attempt" in note for note in raised.value.__notes__)


@pytest.mark.unit
def test_excessive_retry_after_defers_instead_of_shortening_the_wait(tmp_path: Path) -> None:
    """If the server wants a long wait, hand back to Airflow's slower task retry.

    The alternative - capping the sleep below what the server asked for - is exactly
    the kind of limit evasion Wikimedia says it may block clients for.
    """
    wait = MAX_BACKOFF_SECONDS + 60
    client, calls = client_for([httpx.Response(429, headers={"Retry-After": str(int(wait))})])
    sleep, slept = recorder()

    with pytest.raises(httpx.HTTPStatusError) as raised:
        download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert len(calls) == 1
    assert slept == [], "must not sleep for less than the server asked"
    assert any("deferring to the task-level retry" in n for n in raised.value.__notes__)


@pytest.mark.unit
def test_connection_closed_mid_body_is_retried(tmp_path: Path) -> None:
    """Observed for real: the peer closed after 15 MB of a 52 MB file."""
    client, calls = client_for(
        [
            httpx.RemoteProtocolError("peer closed connection without sending complete body"),
            httpx.Response(200, content=GOOD_BODY),
        ]
    )
    sleep, slept = recorder()

    result = download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep)

    assert len(calls) == 2
    assert len(slept) == 1
    assert result.line_count == 2


@pytest.mark.unit
def test_truncated_gzip_is_retried_and_the_partial_file_does_not_survive(
    tmp_path: Path,
) -> None:
    """A body that arrives but is cut short must be re-fetched, not kept.

    The first attempt writes a truncated gzip stream to the destination. The retry must
    replace it entirely - if any of the partial body survived, it could be read as data.
    """
    truncated = GOOD_BODY[: len(GOOD_BODY) // 2]
    client, calls = client_for(
        [
            httpx.Response(200, content=truncated),
            httpx.Response(200, content=GOOD_BODY),
        ]
    )
    sleep, _ = recorder()
    destination = tmp_path / "f.gz"

    result = download_dump(URL, destination, client=client, sleep=sleep)

    assert len(calls) == 2
    assert destination.read_bytes() == GOOD_BODY
    assert result.compressed_bytes == len(GOOD_BODY)


@pytest.mark.unit
def test_persistent_corruption_raises_verification_error(tmp_path: Path) -> None:
    client, _ = client_for([httpx.Response(200, content=b"this is not gzip")])
    sleep, _ = recorder()

    with pytest.raises(DownloadVerificationError, match="failed to decompress"):
        download_dump(URL, tmp_path / "f.gz", client=client, sleep=sleep, max_attempts=2)


@pytest.mark.unit
def test_uncompressed_projectviews_are_verified_without_gzip(tmp_path: Path) -> None:
    client, _ = client_for([httpx.Response(200, content=b"en - 3283165 0\nen.m - 4720588 0\n")])
    sleep, _ = recorder()

    result = download_dump(URL, tmp_path / "pv", client=client, sleep=sleep, compressed=False)

    assert result.line_count == 2
