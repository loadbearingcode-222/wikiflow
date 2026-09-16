"""Object storage access, addressed as S3 and backed by MinIO locally.

Everything here speaks plain S3. The only MinIO-specific detail is the endpoint URL
in config, so moving to real AWS is a configuration change rather than a code change.

The important function is `overwrite_partition`. Idempotency in this pipeline is not
achieved by clever merge logic; it is achieved by making every write replace a whole
partition. Delete-then-write means a re-run cannot append, and therefore cannot
double-count, regardless of how the previous attempt failed.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from wikiflow.config import get_settings

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


def s3_client() -> Any:
    """A boto3 S3 client pointed at whatever endpoint config names."""
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url or None,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        # MinIO requires path-style addressing; AWS accepts it.
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def upload_file(local_path: Path, bucket: str, key: str) -> int:
    """Upload a file and return its size in bytes."""
    client = s3_client()
    client.upload_file(str(local_path), bucket, key)
    return local_path.stat().st_size


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def list_keys(bucket: str, prefix: str) -> Iterator[str]:
    """Yield every key under `prefix`, following pagination."""
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield item["Key"]


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every object under `prefix`. Returns the number deleted.

    Guarded against an empty prefix: passing "" here would silently empty the bucket,
    and the difference between that and a legitimate partition path is one f-string
    interpolating None.
    """
    if not prefix or prefix.strip() in {"", "/"}:
        raise ValueError("refusing to delete with an empty prefix - this would empty the bucket")

    client = s3_client()
    deleted = 0
    batch: list[dict[str, str]] = []

    for key in list_keys(bucket, prefix):
        batch.append({"Key": key})
        if len(batch) == 1000:  # DeleteObjects caps at 1000 per call
            client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            deleted += len(batch)
            batch = []

    if batch:
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)

    return deleted


def overwrite_partition(local_path: Path, bucket: str, partition_prefix: str, key: str) -> int:
    """Replace a partition's contents with one file.

    This is THE idempotency primitive. Any task writing data goes through here, so a
    re-run of any hour replaces that hour rather than adding to it.
    """
    delete_prefix(bucket, partition_prefix)
    return upload_file(local_path, bucket, key)


def configure_duckdb_s3(connection: duckdb.DuckDBPyConnection) -> None:
    """Teach a DuckDB connection to read and write via the configured endpoint.

    DuckDB talks S3 natively through httpfs, so the hourly .gz can be scanned straight
    out of object storage without a separate download step in the transform path.
    """
    settings = get_settings()
    endpoint = settings.s3_endpoint_url.removeprefix("http://").removeprefix("https://")
    use_ssl = settings.s3_endpoint_url.startswith("https://")

    connection.execute("INSTALL httpfs; LOAD httpfs;")
    connection.execute(f"SET s3_endpoint='{endpoint}'")
    connection.execute(f"SET s3_access_key_id='{settings.s3_access_key}'")
    connection.execute(f"SET s3_secret_access_key='{settings.s3_secret_key}'")
    connection.execute(f"SET s3_region='{settings.s3_region}'")
    connection.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    # MinIO requires path-style; AWS tolerates it.
    connection.execute("SET s3_url_style='path'")
