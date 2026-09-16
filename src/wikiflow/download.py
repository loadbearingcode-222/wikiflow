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
import shutil
from dataclasses import dataclass
from pathlib import Path

import httpx

from wikiflow.config import get_settings

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


def download_dump(url: str, destination: Path, *, compressed: bool = True) -> DownloadResult:
    """Stream `url` to `destination` and verify it.

    Args:
        url: The dump URL.
        destination: Local path to stream into. Treat it as scratch - the caller is
            responsible for deleting it, normally in a `finally`.
        compressed: False for `projectviews` files, which are published uncompressed.

    Raises:
        DownloadVerificationError: if any integrity check fails.
        httpx.HTTPStatusError: on a non-2xx response.
    """
    settings = get_settings()
    destination.parent.mkdir(parents=True, exist_ok=True)

    digest = hashlib.sha256()
    written = 0
    declared_length: int | None = None

    with (
        httpx.Client(
            headers={"User-Agent": settings.user_agent},
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client,
        client.stream("GET", url) as response,
    ):
        response.raise_for_status()

        if (header := response.headers.get("content-length")) is not None:
            declared_length = int(header)

        with destination.open("wb") as handle:
            for chunk in response.iter_bytes(_CHUNK):
                handle.write(chunk)
                digest.update(chunk)
                written += len(chunk)

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
        sha256=digest.hexdigest(),
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
