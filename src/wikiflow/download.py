"""Downloading an hourly dump, with verification that it actually arrived intact.

A truncated download is the failure mode that matters here. HTTP will happily hand
back a partial body on a dropped connection, gzip will decompress the prefix without
complaint, and the result is an hour that looks successful but is missing rows. Row
counts would be low, but pageviews volume varies by a factor of 1.6 across the day,
so "low" is not distinguishable from "night-time" without a check.

Four independent verifications, cheapest first:

1. HTTP status is 200.
2. Bytes written match the `Content-Length` header.
3. The gzip stream decompresses end to end, which validates its CRC32 and length
   trailer. This is the one that actually catches truncation.
4. The decompressed content has at least one line.

The gzip check reads the whole 250 MB stream. That costs a few seconds of zlib time
per hour, which is a good trade for eliminating an entire class of silent corruption,
and it produces the row counts the audit table wants anyway.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from wikiflow.config import get_settings

log = logging.getLogger(__name__)

_CHUNK = 1024 * 1024


class DownloadVerificationError(RuntimeError):
    """A download completed but failed an integrity check."""


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Everything the audit table wants to record about one fetch."""

    url: str
    path: Path
    compressed_bytes: int
    decompressed_bytes: int
    line_count: int
    sha256: str

    @property
    def compression_ratio(self) -> float:
        if self.compressed_bytes == 0:
            return 0.0
        return self.decompressed_bytes / self.compressed_bytes


#: Worth retrying: rate limiting and server-side trouble. Any other 4xx - a 404 above
#: all - means the request itself is wrong, and repeating it changes nothing.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

MAX_ATTEMPTS = 5
BASE_BACKOFF_SECONDS = 10.0

#: The longest this function will sleep between attempts. If the server asks for a
#: longer wait than this via Retry-After, the download gives up and leaves the retry to
#: Airflow's own task-level backoff, rather than shortening the wait the server asked
#: for. dumps.wikimedia.org caps clients at 3 connections per IP and states that
#: clients trying to evade its limits may be blocked.
MAX_BACKOFF_SECONDS = 300.0


class _RetryableFailure(Exception):
    """An attempt failed in a way a later attempt might not."""

    def __init__(self, cause: Exception, retry_after: float | None = None) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.retry_after = retry_after


def download_dump(
    url: str,
    destination: Path,
    *,
    compressed: bool = True,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
    max_attempts: int = MAX_ATTEMPTS,
) -> DownloadResult:
    """Stream `url` to `destination` and verify it, retrying transient failures.

    Retries cover three things observed against the real source: `429 Too Many
    Requests`, connections closed mid-body (one attempt received 15 MB of a 52 MB file
    before the peer hung up), and 5xx errors. Each retry re-downloads from scratch - the
    destination is truncated on open, so a partial body from a failed attempt can never
    be mistaken for data.

    Args:
        url: The dump URL.
        destination: Local path to stream into. Treat it as scratch - the caller is
            responsible for deleting it, normally in a `finally`.
        compressed: False for `projectviews` files, which are published uncompressed.
        client: An httpx client to use instead of creating one. Mainly for tests.
        sleep: Called between attempts. Injected so tests do not actually wait.
        max_attempts: Total attempts, including the first.

    Raises:
        DownloadVerificationError: if every attempt fails an integrity check.
        httpx.HTTPStatusError: on a non-retryable status, or a retryable one that
            persisted through every attempt.
        httpx.TransportError: if the connection kept failing.
    """
    settings = get_settings()
    destination.parent.mkdir(parents=True, exist_ok=True)

    owns_client = client is None
    client = client or httpx.Client(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.request_timeout_seconds,
        follow_redirects=True,
    )

    try:
        for attempt in range(1, max_attempts + 1):
            try:
                return _download_once(client, url, destination, compressed=compressed)
            except _RetryableFailure as failure:
                delay = _backoff_seconds(attempt, failure.retry_after)
                out_of_attempts = attempt == max_attempts
                wait_too_long = delay > MAX_BACKOFF_SECONDS

                if out_of_attempts or wait_too_long:
                    reason = (
                        f"gave up after {attempt} attempt(s)"
                        if out_of_attempts
                        else f"server asked to wait {delay:.0f}s, longer than this "
                        "function will; deferring to the task-level retry"
                    )
                    failure.cause.add_note(reason)
                    raise failure.cause from None

                log.warning(
                    "attempt %d/%d for %s failed (%s); retrying in %.0fs",
                    attempt,
                    max_attempts,
                    url,
                    failure.cause,
                    delay,
                )
                sleep(delay)

        raise AssertionError("unreachable: the loop always returns or raises")
    finally:
        if owns_client:
            client.close()


def _backoff_seconds(attempt: int, retry_after: float | None) -> float:
    """How long to wait before the next attempt.

    A server-supplied Retry-After is honoured exactly rather than blended with local
    backoff - it is the server saying how long it needs, and that is the number that
    avoids a second 429. Otherwise exponential: 10s, 20s, 40s, 80s.
    """
    if retry_after is not None:
        return retry_after
    return BASE_BACKOFF_SECONDS * float(2 ** (attempt - 1))


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse Retry-After, which may be delta-seconds or an HTTP date."""
    value = response.headers.get("retry-after")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


def _download_once(
    client: httpx.Client, url: str, destination: Path, *, compressed: bool
) -> DownloadResult:
    """One attempt. Raises _RetryableFailure for anything a retry might fix."""
    digest = hashlib.sha256()
    written = 0
    declared_length: int | None = None

    try:
        with client.stream("GET", url) as response:
            if response.status_code in RETRYABLE_STATUS:
                error = httpx.HTTPStatusError(
                    f"Client error '{response.status_code} {response.reason_phrase}' "
                    f"for url '{url}'",
                    request=response.request,
                    response=response,
                )
                raise _RetryableFailure(error, _retry_after_seconds(response))

            # Anything else non-2xx is permanent and raises straight through.
            response.raise_for_status()

            if (header := response.headers.get("content-length")) is not None:
                declared_length = int(header)

            # "wb" truncates, so a previous attempt's partial body cannot survive.
            with destination.open("wb") as handle:
                for chunk in response.iter_bytes(_CHUNK):
                    handle.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
    except httpx.TransportError as exc:
        # Includes RemoteProtocolError (peer closed mid-body), timeouts, refused
        # connections.
        raise _RetryableFailure(exc) from exc

    try:
        return _verify(
            url,
            destination,
            compressed=compressed,
            written=written,
            declared_length=declared_length,
            sha256=digest.hexdigest(),
        )
    except DownloadVerificationError as exc:
        # A bad transfer is fixed by transferring again.
        raise _RetryableFailure(exc) from exc


def _verify(
    url: str,
    destination: Path,
    *,
    compressed: bool,
    written: int,
    declared_length: int | None,
    sha256: str,
) -> DownloadResult:

    # (2) Size check. Cheap, and catches the common truncation case immediately.
    if declared_length is not None and written != declared_length:
        raise DownloadVerificationError(
            f"{url}: expected {declared_length:,} bytes from Content-Length but wrote "
            f"{written:,}. The download was truncated."
        )

    if written == 0:
        raise DownloadVerificationError(f"{url}: response body was empty")

    # (3) and (4): decompress fully and count lines.
    decompressed_bytes, line_count = _scan_content(destination, compressed=compressed)

    if line_count == 0:
        raise DownloadVerificationError(f"{url}: file contains no lines")

    return DownloadResult(
        url=url,
        path=destination,
        compressed_bytes=written,
        decompressed_bytes=decompressed_bytes,
        line_count=line_count,
        sha256=sha256,
    )


def _scan_content(path: Path, *, compressed: bool) -> tuple[int, int]:
    """Read the whole file, returning (decompressed_bytes, line_count).

    Reading to the end is the point: gzip only validates its CRC32 and ISIZE trailer
    once the final block is consumed, so a partial read would not detect truncation.
    """
    opener = gzip.open if compressed else open

    total = 0
    lines = 0
    last_byte = b""
    try:
        with opener(path, "rb") as stream:
            while chunk := stream.read(_CHUNK):
                total += len(chunk)
                lines += chunk.count(b"\n")
                last_byte = chunk[-1:]
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise DownloadVerificationError(
            f"{path.name}: content failed to decompress ({exc}). "
            "This normally means the download was truncated mid-stream."
        ) from exc

    # A final line without a trailing newline still counts. Tracked during the single
    # pass above rather than by re-reading - these files are ~250 MB decompressed.
    if total > 0 and last_byte != b"\n":
        lines += 1

    return total, lines


def copy_to(result: DownloadResult, target: Path) -> None:
    """Copy a verified download elsewhere without re-verifying it."""
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(result.path, target)
