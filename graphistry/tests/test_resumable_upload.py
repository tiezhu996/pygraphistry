# -*- coding: utf-8 -*-
"""Tests for resumable, transactional graph uploads.

Covers: deterministic chunking, resume from the last confirmed chunk,
idempotent retries (no duplicate staging files/datasets), the validation
gate, cancel semantics, uncertain-commit protection, and the invariant that
a failed/cancelled attempt never disturbs the previously uploaded dataset.
"""

import types
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
import requests

import graphistry
from graphistry.ArrowFileUploader import ArrowFileUploader
from graphistry.arrow_uploader import ArrowUploader
from graphistry.tests.common import NoAuthTestCase
from graphistry.upload_session import (
    ChunkState,
    ResumableUploadError,
    ResumableUploadSession,
    SessionState,
    UncertainCommitError,
    UploadCancelledError,
    UploadValidationError,
    plan_chunks,
)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeFileUploader:
    """Records create/post traffic and can inject transport or HTTP failures."""

    def __init__(self, conn_fail_on_post=(), invalid_response_files=()):
        self._counters = {"edges": 0, "nodes": 0}
        self.created = []  # [(file_id, opts)]
        self.posts = []  # [file_id]
        self.post_url_opts = []
        self._conn_fail = set(conn_fail_on_post)
        self._invalid = set(invalid_response_files)
        self.http_fail = {}  # file_id -> Exception

    def create_file(self, file_opts):
        kind = file_opts["name"].rsplit(" ", 1)[-1]
        file_id = f"{kind}-{self._counters[kind]}"
        self._counters[kind] += 1
        self.created.append((file_id, file_opts))
        return file_id

    def post_arrow(self, arr, file_id, url_opts="erase=true"):
        self.posts.append(file_id)
        self.post_url_opts.append(url_opts)
        if file_id in self._conn_fail:
            raise requests.exceptions.ConnectionError(f"simulated outage on {file_id}")
        if file_id in self.http_fail:
            raise self.http_fail[file_id]
        if file_id in self._invalid:
            return {"is_valid": False, "is_uploaded": False, "errors": ["bad data"]}
        return {"is_valid": True, "is_uploaded": True, "errors": []}

    def repost_arrow(self, arr, file_id, file_opts=None, upload_url_opts="erase=true"):
        # Session reuses the same file_id; 404 reconciliation is unit-tested
        # against the real ArrowFileUploader.repost_arrow below.
        return file_id, self.post_arrow(arr, file_id, upload_url_opts)


class FakeDatasetCreator:
    def __init__(self, uploader):
        self._uploader = uploader
        self.calls = []
        self.side_effect = None

    def __call__(self, payload, validate="autofix", warn=True):
        self.calls.append(dict(payload))
        if self.side_effect is not None:
            raise self.side_effect
        self._uploader.dataset_id = "ds-new"
        return {"success": True, "data": {"dataset_id": "ds-new"}}


def make_uploader(edges, nodes=None):
    au = types.SimpleNamespace()
    au.edges = edges
    au.nodes = nodes
    au.name = "g"
    au.description = ""
    au.org_name = None
    au.metadata = {"agent": "pygraphistry"}
    au.node_encodings = {"bindings": {}}
    au.edge_encodings = {"bindings": {}}
    au.certificate_validation = False
    au.token = "tok"
    au.server_base_path = "https://example"
    # Pre-existing LIVE version that the transaction must never disturb.
    au.dataset_id = "ds-old"
    au.edges_file_id = "old-edge"
    au.nodes_file_id = "old-node"
    au.resumable_edge_file_ids = None
    au.resumable_node_file_ids = None
    creator = FakeDatasetCreator(au)
    au.create_dataset = creator
    return au, creator


def edges_table(rows=9):
    return pa.Table.from_pandas(pd.DataFrame({"s": range(rows), "d": range(rows, 2 * rows)}))


def nodes_table(rows=4):
    return pa.Table.from_pandas(pd.DataFrame({"id": range(rows), "label": [f"n{i}" for i in range(rows)]}))


def mb_for_rows_per_chunk(table, rows_per_chunk):
    """Fractional MB target that yields approximately rows_per_chunk rows/chunk."""
    per_row = max(1, int(table.nbytes) // table.num_rows)
    return (per_row * rows_per_chunk) / (1024.0 * 1024.0)


def make_session(au, afu, table, chunk_mb):
    session = ResumableUploadSession(
        uploader=au,
        file_uploader=afu,
        chunk_size_mb=chunk_mb,
    )
    return session


@pytest.fixture
def verify_get_ok():
    with mock.patch("graphistry.upload_session.requests.get") as mocked_get:
        mocked_get.return_value = mock.Mock(status_code=200)
        yield mocked_get


# --------------------------------------------------------------------------- #
# Chunking
# --------------------------------------------------------------------------- #


class TestChunkPlanning:
    def test_small_and_empty_tables_are_single_chunk(self):
        t = pa.Table.from_pandas(pd.DataFrame({"x": [1, 2, 3]}))
        plans = plan_chunks("edges", t, target_bytes=64 * 1024 * 1024)
        assert len(plans) == 1
        assert plans[0].offset == 0 and plans[0].length == 3

        empty = t.slice(0, 0)
        empty_plans = plan_chunks("edges", empty, target_bytes=1)
        assert len(empty_plans) == 1 and empty_plans[0].length == 0

    def test_plans_are_gap_free_and_deterministic(self):
        t = edges_table(9)
        p1 = plan_chunks("edges", t, target_bytes=1)
        p2 = plan_chunks("edges", t, target_bytes=1)
        assert p1 == p2
        assert [p.index for p in p1] == list(range(len(p1)))

        offsets = [p.offset for p in p1]
        lengths = [p.length for p in p1]
        assert offsets[0] == 0
        assert all(offsets[i + 1] == offsets[i] + lengths[i] for i in range(len(p1) - 1))
        assert sum(lengths) == t.num_rows

        rebuilt = pa.concat_tables([t.slice(p.offset, p.length) for p in p1])
        assert rebuilt.to_pydict() == t.to_pydict()

    def test_chunk_hashes_pin_content(self):
        t = edges_table(6)
        plans = plan_chunks("edges", t, target_bytes=1)
        assert len(plans) == 6
        assert len({p.sha256 for p in plans}) == 6
        for p in plans:
            assert p.sha256 == plan_chunks(
                "edges", t.slice(p.offset, p.length), target_bytes=10**12
            )[0].sha256

    def test_rejects_non_positive_target(self):
        t = edges_table(3)
        with pytest.raises(ValueError):
            plan_chunks("edges", t, target_bytes=0)


# --------------------------------------------------------------------------- #
# Session lifecycle
# --------------------------------------------------------------------------- #


class TestResumableSession:
    def test_full_success_stages_then_commits_once(self, verify_get_ok):
        edges = edges_table(9)
        nodes = nodes_table(4)
        au, creator = make_uploader(edges, nodes)
        afu = FakeFileUploader()
        session = make_session(
            au,
            afu,
            edges,
            mb_for_rows_per_chunk(edges, 3),
        )
        # Nodes chunk plan uses the same MB target; record expectations from journal.
        expected_edge_chunks = len(session.edges_progress)
        expected_node_chunks = len(session.nodes_progress)
        assert expected_edge_chunks >= 2 and expected_node_chunks >= 1

        finalize = mock.Mock(return_value="PLOT")
        result = session.run(finalize=finalize)

        assert result == "PLOT"
        assert session.state == SessionState.COMMITTED
        assert session.dataset_id == "ds-new"

        assert len(afu.created) == expected_edge_chunks + expected_node_chunks
        assert len(afu.posts) == expected_edge_chunks + expected_node_chunks
        assert all(opt == "erase=true" for opt in afu.post_url_opts)

        assert len(creator.calls) == 1
        payload = creator.calls[0]
        assert payload["edge_files"] == [f"edges-{i}" for i in range(expected_edge_chunks)]
        assert payload["node_files"] == [f"nodes-{i}" for i in range(expected_node_chunks)]

        assert au.edges_file_id == "edges-0"
        assert au.nodes_file_id == "nodes-0"
        assert au.resumable_edge_file_ids == payload["edge_files"]
        assert au.resumable_node_file_ids == payload["node_files"]
        finalize.assert_called_once()
        verify_get_ok.assert_called_once()

    def test_transport_failure_resumes_without_duplicates(self, verify_get_ok):
        edges = edges_table(9)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader(conn_fail_on_post={"edges-1"})
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 3))
        chunk_count = len(session.edges_progress)
        assert chunk_count == 3
        finalize = mock.Mock(return_value="PLOT")

        # First attempt dies staging chunk index 1.
        with pytest.raises(ResumableUploadError) as exc_info:
            session.run(finalize=finalize)
        assert not isinstance(exc_info.value, UncertainCommitError)
        assert creator.calls == []  # never reached the version switch
        assert au.dataset_id == "ds-old"  # live version untouched

        states = [p.state for p in session.edges_progress]
        assert states == [ChunkState.CONFIRMED, ChunkState.FILE_CREATED, ChunkState.PENDING]
        assert session.edges_progress[1].file_id == "edges-1"
        assert [fid for fid, _ in afu.created] == ["edges-0", "edges-1"]

        # Network restored: resume continues from the last confirmed chunk.
        afu._conn_fail.clear()
        result = session.resume(finalize=finalize)

        assert result == "PLOT"
        assert session.state == SessionState.COMMITTED
        # Confirmed chunk 0 not resent; chunk 1 reused its file_id (no extra create).
        assert [fid for fid, _ in afu.created] == ["edges-0", "edges-1", "edges-2"]
        assert afu.posts == ["edges-0", "edges-1", "edges-1", "edges-2"]
        assert len(creator.calls) == 1
        assert au.dataset_id == "ds-new"
        finalize.assert_called_once()

    def test_resume_after_success_is_a_noop(self, verify_get_ok):
        edges = edges_table(3)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader()
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 1))
        finalize = mock.Mock(return_value="PLOT")

        assert session.run(finalize=finalize) == "PLOT"
        posts_after_commit = len(afu.posts)
        creates_after_commit = len(afu.created)
        finalize2 = mock.Mock(return_value="SHOULD_NOT_RUN")

        assert session.resume(finalize=finalize2) == "PLOT"
        assert len(afu.posts) == posts_after_commit
        assert len(afu.created) == creates_after_commit
        assert len(creator.calls) == 1
        finalize2.assert_not_called()

    def test_validation_failure_blocks_commit(self, verify_get_ok):
        edges = edges_table(6)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader(invalid_response_files={"edges-1"})
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 2))
        assert len(session.edges_progress) == 3

        with pytest.raises(UploadValidationError):
            session.run()
        assert creator.calls == []
        assert session.state == SessionState.STAGING
        assert au.dataset_id == "ds-old"

    def test_cancel_abandons_without_touching_live_version(self, verify_get_ok):
        edges = edges_table(3)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader()
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 1))
        finalize = mock.Mock()

        session.cancel()
        assert session.state == SessionState.ABORTED
        with pytest.raises(UploadCancelledError):
            session.run(finalize=finalize)
        with pytest.raises(UploadCancelledError):
            session.resume()
        assert creator.calls == []
        assert afu.created == []
        finalize.assert_not_called()
        assert au.dataset_id == "ds-old"

    def test_uncertain_commit_requires_explicit_retry(self, verify_get_ok):
        edges = edges_table(2)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader()
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 1))
        finalize = mock.Mock(return_value="PLOT")
        creator.side_effect = requests.exceptions.ConnectionError("lost commit response")

        with pytest.raises(UncertainCommitError):
            session.run(finalize=finalize)
        assert session.state == SessionState.COMMITTING
        assert len(creator.calls) == 1

        # Plain resume must NOT risk a duplicate dataset.
        with pytest.raises(UncertainCommitError):
            session.resume()
        assert len(creator.calls) == 1
        finalize.assert_not_called()

        # Explicit retry after confirming the server did not create it.
        creator.side_effect = None
        assert session.resume(finalize=finalize, retry_commit=True) == "PLOT"
        assert len(creator.calls) == 2
        assert session.state == SessionState.COMMITTED
        finalize.assert_called_once()

    def test_definitive_commit_rejection_can_resume(self, verify_get_ok):
        edges = edges_table(2)
        au, creator = make_uploader(edges, None)
        afu = FakeFileUploader()
        session = make_session(au, afu, edges, mb_for_rows_per_chunk(edges, 1))
        finalize = mock.Mock(return_value="PLOT")
        response = mock.Mock(status_code=400)
        creator.side_effect = requests.HTTPError(response=response)

        with pytest.raises(ResumableUploadError) as exc_info:
            session.run()
        assert not isinstance(exc_info.value, UncertainCommitError)
        assert session.state == SessionState.STAGED
        assert len(creator.calls) == 1

        creator.side_effect = None
        assert session.resume(finalize=finalize) == "PLOT"
        assert len(creator.calls) == 2  # staging was reused; only commit retried
        # Confirmed chunks are not re-staged on resume.
        assert afu.posts == ["edges-0", "edges-1"]
        assert len(afu.created) == 2
        finalize.assert_called_once()


# --------------------------------------------------------------------------- #
# ArrowFileUploader 404 reconciliation
# --------------------------------------------------------------------------- #


class TestRepostArrowReconciliation:
    def _afu(self):
        stub = types.SimpleNamespace(
            token="dummy",
            server_base_path="https://example",
            certificate_validation=False,
        )
        return ArrowFileUploader(stub)

    def test_404_recreates_file_once(self):
        afu = self._afu()
        arr = pa.Table.from_pandas(pd.DataFrame({"x": [1]}))
        lost_response = mock.Mock(status_code=404)
        afu.post_arrow = mock.Mock(
            side_effect=[
                requests.HTTPError(response=lost_response),
                {"is_valid": True, "is_uploaded": True, "errors": []},
            ]
        )
        afu.create_file = mock.Mock(return_value="new-file")

        file_id, response = afu.repost_arrow(arr, "lost-file", {"name": "g edges"})

        assert file_id == "new-file"
        assert response["is_valid"] is True
        afu.create_file.assert_called_once_with({"name": "g edges"})
        assert [c.args[1] for c in afu.post_arrow.call_args_list] == ["lost-file", "new-file"]

    def test_other_http_errors_propagate(self):
        afu = self._afu()
        arr = pa.Table.from_pandas(pd.DataFrame({"x": [1]}))
        afu.post_arrow = mock.Mock(
            side_effect=requests.HTTPError(response=mock.Mock(status_code=403))
        )
        afu.create_file = mock.Mock()

        with pytest.raises(requests.HTTPError):
            afu.repost_arrow(arr, "some-file")
        afu.create_file.assert_not_called()


# --------------------------------------------------------------------------- #
# Plot integration
# --------------------------------------------------------------------------- #


def test_public_exception_exports():
    assert graphistry.ResumableUploadError is ResumableUploadError
    assert graphistry.UncertainCommitError is UncertainCommitError
    assert graphistry.UploadCancelledError is UploadCancelledError
    assert graphistry.ResumableUploadSession is ResumableUploadSession


class TestPlotResumableIntegration(NoAuthTestCase):
    def test_resumable_requires_as_files(self):
        g = graphistry.edges(pd.DataFrame({"s": [0, 1], "d": [1, 0]}), "s", "d")
        with pytest.raises(ValueError, match="as_files"):
            g.plot(render="g", as_files=False, resumable=True)

    def test_end_to_end_resumable_upload_switches_only_after_commit(self):
        edges_df = pd.DataFrame({"s": [0, 1], "d": [1, 0]})
        original = graphistry.edges(edges_df, "s", "d")

        fake_afu = FakeFileUploader()

        original_factory = ArrowUploader.make_resumable_session

        def factory_with_fake_files(self, **kwargs):  # noqa: N805
            session = original_factory(self, **kwargs)
            session._file_uploader = fake_afu  # inject network-free file uploader
            return session

        def fake_post(url, **kwargs):
            resp = mock.Mock()
            resp.status_code = 200
            if url.endswith("/api/v2/upload/datasets/"):
                resp.json.return_value = {
                    "success": True,
                    "data": {"dataset_id": "ds-e2e"},
                }
            else:
                resp.json.return_value = {}
            return resp

        with mock.patch.object(
            ArrowUploader, "make_resumable_session", factory_with_fake_files
        ), mock.patch("graphistry.arrow_uploader.requests.post", side_effect=fake_post), \
                mock.patch("graphistry.upload_session.requests.get") as mocked_get, \
                mock.patch.object(
                    graphistry.pygraphistry.PyGraphistry,
                    "refresh",
                    return_value="faketoken",
                ):
            mocked_get.return_value = mock.Mock(status_code=200)
            uploaded = original.upload(resumable=True)

        assert uploaded._dataset_id == "ds-e2e"
        assert uploaded._edges_file_id == "edges-0"
        assert uploaded._url is not None
        # The caller's prior graph is untouched by the whole transaction.
        assert original._dataset_id is None
        # One edges chunk (no nodes), exactly one dataset commit.
        assert [fid for fid, _ in fake_afu.created] == ["edges-0"]
