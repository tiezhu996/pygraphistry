"""Resumable, transactional graph uploads (client-side staging + single commit).

Opt-in via ``PlotterBase.upload(resumable=True)``; the default upload path does
not use this module.

Transaction model
-----------------

A graph upload is split into deterministic, content-addressed row chunks. Each
chunk is staged into its own server-side File via the existing File REST API
(``POST /api/v2/files/`` + ``POST /api/v2/upload/files/{file_id}``). The live
dataset is never touched: a new dataset is created exactly once, and only after
every chunk is confirmed and the whole-content validation gate passes.

Per-chunk state machine::

    PENDING -> FILE_CREATED(file_id) -> CONFIRMED(file_id, response)

Session state machine::

    STAGING -> STAGED -> COMMITTING -> COMMITTED
                       \\-> STAGED        (definitive server rejection)
    ABORTED (cancel)

Retries never resend a CONFIRMED chunk and never recreate its file. When a chunk
is FILE_CREATED but the upload response was lost, resume re-posts to the *same*
file_id (``erase=true``), recreating the file only on a definitive 404. A
connection error while creating the dataset is inherently ambiguous (the server
may or may not have created it), so resume refuses to re-commit unless the
caller explicitly passes ``retry_commit=True``; this is what prevents duplicate
datasets on retry.

Journal state is in-memory and single-process by design.
"""

import enum
import hashlib
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Literal, Optional, TypeVar

import pyarrow as pa
import requests

from .otel import inject_trace_headers
from .util import setup_logger

if TYPE_CHECKING:
    from .ArrowFileUploader import ArrowFileUploader
    from .arrow_uploader import ArrowUploader

logger = setup_logger(__name__)

T = TypeVar("T")

TableKind = Literal["edges", "nodes"]

DEFAULT_CHUNK_SIZE_MB: int = 64
_BYTES_PER_MB: int = 1024 * 1024


class ResumableUploadError(Exception):
    """Upload did not complete; ``self.session`` can resume from the last confirmed chunk."""

    def __init__(self, message: str, session: "ResumableUploadSession") -> None:
        super().__init__(message)
        self._session = session

    @property
    def session(self) -> "ResumableUploadSession":
        return self._session


class UploadCancelledError(ResumableUploadError):
    """Session was cancelled; resume() is a no-go and the old dataset remains live."""


class UncertainCommitError(ResumableUploadError):
    """Dataset creation request was lost in transit; the server-side outcome is unknown.

    A plain resume() refuses to re-commit (that could create a second dataset).
    Call resume(retry_commit=True) only after confirming the server did not
    create the dataset.
    """


class UploadValidationError(ResumableUploadError):
    """Staged chunks failed the whole-content validation gate; commit was not attempted."""


class ChunkState(enum.Enum):
    PENDING = "pending"
    FILE_CREATED = "file_created"
    CONFIRMED = "confirmed"


class SessionState(enum.Enum):
    STAGING = "staging"
    STAGED = "staged"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class ChunkPlan:
    """Deterministic description of one row slice of a source table."""

    kind: TableKind
    index: int
    offset: int
    length: int
    sha256: str


@dataclass
class ChunkProgress:
    """Mutable per-chunk journal entry."""

    plan: ChunkPlan
    state: ChunkState = ChunkState.PENDING
    file_id: Optional[str] = None
    response: Optional[Dict[str, object]] = None


def table_content_hash(table: pa.Table) -> str:
    """Full-content SHA-256 hex digest of an Arrow table.

    Serializes the table through the deterministic Arrow IPC stream format so
    that zero-copy row slices (which share their parent buffers) get distinct
    hashes instead of inheriting the full parent buffer.
    """
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def plan_chunks(kind: TableKind, table: pa.Table, target_bytes: int) -> List[ChunkPlan]:
    """Split ``table`` into deterministic, gap-free row chunks near ``target_bytes``.

    Empty/small tables (or non-positive ``target_bytes``) yield a single chunk so
    the wire shape stays identical to the legacy whole-table upload.
    """
    if target_bytes <= 0:
        raise ValueError("chunk target size must be positive")

    num_rows = table.num_rows
    nbytes = int(getattr(table, "nbytes", 0) or 0)

    if num_rows <= 1 or nbytes <= target_bytes:
        return [
            ChunkPlan(
                kind=kind,
                index=0,
                offset=0,
                length=num_rows,
                sha256=table_content_hash(table),
            )
        ]

    bytes_per_row = max(1, nbytes // num_rows)
    rows_per_chunk = int(max(1, target_bytes // bytes_per_row))

    plans: List[ChunkPlan] = []
    index = 0
    for offset in range(0, num_rows, rows_per_chunk):
        length = min(rows_per_chunk, num_rows - offset)
        chunk = table.slice(offset, length)
        plans.append(
            ChunkPlan(
                kind=kind,
                index=index,
                offset=offset,
                length=length,
                sha256=table_content_hash(chunk),
            )
        )
        index += 1
    return plans


class ResumableUploadSession:
    """In-memory transaction for one staged graph upload.

    Use :meth:`run` (or :meth:`resume`) to drive the session to COMMITTED and,
    on success, run the ``finalize`` callback (plot URL generation and binding
    of the new dataset onto a fresh Plottable). The caller's original graph is
    never mutated: finalize only runs after the commit succeeded.
    """

    def __init__(
        self,
        uploader: "ArrowUploader",
        file_uploader: "ArrowFileUploader",
        chunk_size_mb: float = DEFAULT_CHUNK_SIZE_MB,
        validate: str = "autofix",
        warn: bool = True,
    ) -> None:
        if uploader.edges is None:
            raise ValueError("edges table is None")
        if chunk_size_mb <= 0:
            raise ValueError("chunk_size_mb must be positive")

        self._uploader = uploader
        self._file_uploader = file_uploader
        self._validate_mode = validate
        self._warn = warn
        self._target_bytes = int(chunk_size_mb * _BYTES_PER_MB)

        self._lock = threading.RLock()
        self._state = SessionState.STAGING
        self._dataset_id: Optional[str] = None
        self._finalized: object = None
        self._finalized_set = False

        self._edges_progress = [
            ChunkProgress(plan=p)
            for p in plan_chunks("edges", uploader.edges, self._target_bytes)
        ]
        self._nodes_progress: List[ChunkProgress] = []
        if uploader.nodes is not None:
            self._nodes_progress = [
                ChunkProgress(plan=p)
                for p in plan_chunks("nodes", uploader.nodes, self._target_bytes)
            ]

        nodes_hash = (
            table_content_hash(uploader.nodes) if uploader.nodes is not None else "-"
        )
        digest = hashlib.sha256()
        digest.update(table_content_hash(uploader.edges).encode("utf-8"))
        digest.update(nodes_hash.encode("utf-8"))
        digest.update(str(uploader.name).encode("utf-8"))
        self._session_id = digest.hexdigest()[:16]

    @property
    def id(self) -> str:
        return self._session_id

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def dataset_id(self) -> Optional[str]:
        return self._dataset_id

    @property
    def edges_progress(self) -> List[ChunkProgress]:
        return list(self._edges_progress)

    @property
    def nodes_progress(self) -> List[ChunkProgress]:
        return list(self._nodes_progress)

    def _table(self, kind: TableKind) -> pa.Table:
        table = self._uploader.edges if kind == "edges" else self._uploader.nodes
        assert table is not None
        return table

    def run(self, finalize: Optional[Callable[[], T]] = None) -> Optional[T]:
        """Run the session to completion; equivalent to ``resume()`` on a fresh session."""
        return self.resume(finalize=finalize)

    def resume(
        self,
        finalize: Optional[Callable[[], T]] = None,
        retry_commit: bool = False,
    ) -> Optional[T]:
        """Continue from the last confirmed chunk and (on success) run ``finalize``.

        CONFIRMED chunks are never re-uploaded. Raises
        :class:`UploadCancelledError` after cancel(), :class:`UncertainCommitError`
        when the dataset-creation response was lost (use ``retry_commit=True``
        after manually confirming the server has no such dataset), or another
        :class:`ResumableUploadError` for recoverable transport failures.
        """
        with self._lock:
            if self._state == SessionState.ABORTED:
                raise UploadCancelledError(
                    "Upload session was cancelled; the previous dataset remains live.",
                    self,
                )

            if self._state == SessionState.COMMITTED:
                return self._run_finalize(finalize)

            if self._state == SessionState.COMMITTING:
                if not retry_commit:
                    raise UncertainCommitError(
                        "Dataset creation outcome is unknown after a transport error; "
                        "verify on the server whether the dataset was created, then call "
                        "resume(retry_commit=True) to re-commit if it was not.",
                        self,
                    )
                self._commit()
            else:
                # STAGING / STAGED
                self._stage_all()
                self._validation_gate()
                self._commit()

            assert self._state == SessionState.COMMITTED
            return self._run_finalize(finalize)

    def cancel(self) -> None:
        """Abandon the session. Never commits; the old dataset stays untouched."""
        with self._lock:
            if self._state == SessionState.COMMITTED:
                logger.info("Session %s already committed; cancel() ignored", self._session_id)
                return
            logger.info("Cancelling upload session %s", self._session_id)
            self._state = SessionState.ABORTED

    def _stage_all(self) -> None:
        for progress in self._edges_progress:
            self._send_chunk(progress)
        for progress in self._nodes_progress:
            self._send_chunk(progress)

    def _file_opts(self, kind: TableKind) -> Dict[str, str]:
        opts = {"name": f"{self._uploader.name} {kind}"}
        if self._uploader.org_name:
            opts["org_name"] = self._uploader.org_name
        return opts

    def _send_chunk(self, progress: ChunkProgress) -> None:
        if progress.state == ChunkState.CONFIRMED:
            return

        plan = progress.plan
        chunk = self._table(plan.kind).slice(plan.offset, plan.length)
        file_opts = self._file_opts(plan.kind)
        afu = self._file_uploader

        try:
            if progress.state == ChunkState.FILE_CREATED:
                # Reuse the acknowledged file_id (erase=true repost is idempotent; a 404 recreates it).
                assert progress.file_id is not None
                progress.file_id, response = afu.repost_arrow(
                    chunk, progress.file_id, file_opts, "erase=true"
                )
            else:
                # Transport failure here stays PENDING: create outcome unknown, so no file_id is remembered.
                progress.file_id = afu.create_file(file_opts)
                progress.state = ChunkState.FILE_CREATED
                # Transport failure here stays FILE_CREATED for resume to retry.
                response = afu.post_arrow(chunk, progress.file_id, "erase=true")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            raise ResumableUploadError(
                f"Transport failure while staging {plan.kind} chunk {plan.index} "
                f"(session {self._session_id}); call session.resume() to continue.",
                self,
            ) from exc

        progress.response = response
        progress.state = ChunkState.CONFIRMED
        logger.debug(
            "Confirmed %s chunk %s/%s as file %s (session %s)",
            plan.kind,
            plan.index + 1,
            len(self._edges_progress if plan.kind == "edges" else self._nodes_progress),
            progress.file_id,
            self._session_id,
        )

    def _validation_gate(self) -> None:
        self._validate_kind("edges", self._uploader.edges, self._edges_progress)
        if self._uploader.nodes is not None:
            self._validate_kind("nodes", self._uploader.nodes, self._nodes_progress)
        self._state = SessionState.STAGED

    def _validate_kind(
        self, kind: TableKind, table: pa.Table, progresses: List[ChunkProgress]
    ) -> None:
        total_rows = 0
        file_ids: List[str] = []
        for progress in progresses:
            plan = progress.plan
            if progress.state != ChunkState.CONFIRMED:
                raise UploadValidationError(
                    f"{kind} chunk {plan.index} is not confirmed", self
                )
            assert progress.file_id is not None
            if not (progress.response or {}).get("is_valid", False):
                raise UploadValidationError(
                    f"Server rejected {kind} chunk {plan.index} contents", self
                )
            chunk = table.slice(plan.offset, plan.length)
            if chunk.schema != table.schema:
                raise UploadValidationError(
                    f"{kind} chunk {plan.index} schema drifted from source table", self
                )
            if table_content_hash(chunk) != plan.sha256:
                raise UploadValidationError(
                    f"{kind} chunk {plan.index} content hash drifted from journal", self
                )
            total_rows += plan.length
            file_ids.append(progress.file_id)

        if total_rows != table.num_rows:
            raise UploadValidationError(
                f"Staged {kind} row count {total_rows} != source {table.num_rows}", self
            )
        if len(set(file_ids)) != len(file_ids):
            raise UploadValidationError(
                f"Duplicate staging file ids among {kind} chunks", self
            )

    def _ordered_file_ids(self, progresses: List[ChunkProgress]) -> List[str]:
        ordered = sorted(progresses, key=lambda p: p.plan.index)
        file_ids: List[str] = []
        for progress in ordered:
            assert progress.file_id is not None
            file_ids.append(progress.file_id)
        return file_ids

    def _commit(self) -> None:
        self._state = SessionState.COMMITTING

        edge_file_ids = self._ordered_file_ids(self._edges_progress)
        node_file_ids = self._ordered_file_ids(self._nodes_progress)

        payload: Dict[str, object] = {
            "node_encodings": self._uploader.node_encodings,
            "edge_encodings": self._uploader.edge_encodings,
            "metadata": self._uploader.metadata,
            "name": self._uploader.name,
            "description": self._uploader.description,
            "edge_files": edge_file_ids,
        }
        if self._uploader.nodes is not None:
            payload["node_files"] = node_file_ids

        try:
            self._uploader.create_dataset(
                payload, validate=self._validate_mode, warn=self._warn
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            raise UncertainCommitError(
                f"Transport error while creating dataset for session {self._session_id}; "
                "server-side outcome unknown.",
                self,
            ) from exc
        except Exception as exc:
            # Definitive rejection (nothing created): reset to STAGED so resume may re-commit.
            self._state = SessionState.STAGED
            raise ResumableUploadError(
                f"Dataset commit failed for session {self._session_id}: {exc}", self
            ) from exc

        self._dataset_id = self._uploader.dataset_id
        self._uploader.edges_file_id = edge_file_ids[0]
        self._uploader.resumable_edge_file_ids = list(edge_file_ids)
        if self._uploader.nodes is not None:
            self._uploader.nodes_file_id = node_file_ids[0]
            self._uploader.resumable_node_file_ids = list(node_file_ids)

        self._state = SessionState.COMMITTED
        logger.info(
            "Committed resumable upload session %s as dataset %s",
            self._session_id,
            self._dataset_id,
        )
        self._verify_commit()

    def _verify_commit(self) -> None:
        """Best-effort post-commit check via the dataset metadata endpoint.

        A failure here never rolls back (the dataset was created) nor blocks
        finalization; it only logs so operators can investigate.
        """
        try:
            url = (
                f"{self._uploader.server_base_path}"
                f"/api/v2/upload/datasets/{self._dataset_id}"
            )
            res = requests.get(
                url,
                verify=self._uploader.certificate_validation,
                headers=inject_trace_headers(
                    {"Authorization": f"Bearer {self._uploader.token}"}
                ),
            )
            if not (200 <= res.status_code < 300):
                logger.warning(
                    "Post-commit dataset verification returned HTTP %s for %s",
                    res.status_code,
                    self._dataset_id,
                )
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Post-commit dataset verification failed for %s: %s",
                self._dataset_id,
                exc,
            )

    def _run_finalize(self, finalize: Optional[Callable[[], T]]) -> Optional[T]:
        if self._finalized_set:
            # Idempotent resume after success: zero additional work.
            return self._finalized
        if finalize is not None:
            self._finalized = finalize()
            self._finalized_set = True
        return self._finalized
