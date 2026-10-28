from collections.abc import Sequence
from types import TracebackType
from typing import Any, Protocol, Self

__version__: str

class ArrowSchema(Protocol):
    def __arrow_c_schema__(self) -> object: ...

class ArrowArrayStream(Protocol):
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

class ArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema: object | None = None) -> tuple[object, object]: ...

class AroyoError(Exception):
    """Raised for any storage, Parquet or checkpoint-state error."""

class S3Config:
    """Connection settings for one S3-compatible endpoint."""

    def __init__(
        self,
        *,
        read_env: bool = False,
        bucket: str | None = None,
        region: str | None = None,
        endpoint: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        allow_http: bool | None = None,
        virtual_hosted_style: bool | None = None,
    ) -> None: ...
    @staticmethod
    def from_env(
        *,
        bucket: str | None = None,
        region: str | None = None,
        endpoint: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        allow_http: bool | None = None,
        virtual_hosted_style: bool | None = None,
    ) -> S3Config:
        """Reads ``AWS_*`` environment variables; keyword arguments override them."""
    @property
    def bucket(self) -> str | None: ...
    @property
    def region(self) -> str | None: ...
    @property
    def endpoint(self) -> str | None: ...
    def __getnewargs_ex__(self) -> tuple[tuple[()], dict[str, object]]:
        """Pickle support; the rebuilt config carries the secrets."""

class WriterState:
    """Checkpoint state of one upload: the parts acknowledged plus the trailing bytes."""

    @property
    def path(self) -> str: ...
    @property
    def upload_id(self) -> str: ...
    @property
    def part_size(self) -> int: ...
    @property
    def parts(self) -> list[tuple[int, str]]:
        """``(part_number, etag)`` for every acknowledged part, in order."""
    @property
    def uploaded_bytes(self) -> int: ...
    @property
    def rows(self) -> int:
        """Rows the finished file will hold."""
    @property
    def expected_size(self) -> int:
        """Size of the object once finished."""
    @property
    def trailing_bytes(self) -> bytes: ...
    @property
    def is_pending_commit(self) -> bool:
        """True after ``close``: only the completion is pending."""
    def to_bytes(self) -> bytes: ...
    @staticmethod
    def from_bytes(data: bytes) -> WriterState: ...
    def __eq__(self, other: object) -> bool: ...

class PendingCommit:
    """A closed file whose multipart upload still has to be completed."""

    def commit(self) -> None:
        """Completes the upload. Safe to call again after a failure."""
    @property
    def state(self) -> WriterState: ...
    @property
    def path(self) -> str: ...
    @property
    def rows(self) -> int: ...
    @property
    def expected_size(self) -> int: ...
    def to_bytes(self) -> bytes:
        """The same blob as ``WriterState.to_bytes``; load it with ``WriterState.from_bytes``."""

class ParquetStreamWriter:
    """Streams record batches into one Parquet multipart upload."""

    def __init__(
        self,
        config: S3Config,
        path: str,
        schema: ArrowSchema,
        *,
        part_size: int = ...,
        row_group_size: int = ...,
        max_row_group_rows: int = ...,
        max_in_flight_parts: int = ...,
        compression: str = "snappy",
    ) -> None: ...
    def write(self, data: ArrowArray | ArrowArrayStream | Sequence[Any]) -> None:
        """Writes a record batch, a table or a record-batch stream."""
    def checkpoint(self) -> WriterState:
        """Closes the row group in progress and returns the state that finishes the file from here."""
    def close(self) -> PendingCommit:
        """Writes the final footer as the last part; the upload is completed by ``PendingCommit.commit``."""
    def abort(self) -> None:
        """Aborts the upload and discards every part."""
    @property
    def path(self) -> str: ...
    @property
    def upload_id(self) -> str: ...
    @property
    def rows_written(self) -> int:
        """Rows in closed row groups."""
    @property
    def rows_pending(self) -> int:
        """Rows buffered for the row group in progress."""
    @property
    def bytes_written(self) -> int:
        """Encoded Parquet bytes produced so far."""
    @property
    def part_count(self) -> int:
        """Parts uploaded or uploading."""
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool: ...

def finish(config: S3Config, state: WriterState) -> None:
    """Uploads the trailing bytes of ``state`` and completes the upload.

    Run it from a new process after a crash with the last recorded state. The
    writer that produced the state must be stopped first. Safe to call again:
    an object of the expected size counts as success.
    """

def abort(config: S3Config, state: WriterState) -> None:
    """Aborts the upload described by ``state``, discarding every part."""

def read_parquet(
    config: S3Config,
    path: str,
    *,
    columns: Sequence[str] | None = None,
    concurrency: int = 4,
    batch_size: int = 65536,
) -> ArrowArrayStream:
    """Opens a Parquet object as a record-batch stream (``__arrow_c_stream__``).

    ``concurrency`` row groups are fetched and decoded ahead of the consumer.
    Columns come back in file order whatever the order of ``columns``.
    """
