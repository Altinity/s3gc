from argparse import ArgumentError
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


class QueryResult:
    def __init__(self, value):
        self.result_rows = [(value,)]


class FakeStream:
    def __init__(self, blocks):
        self.blocks = blocks

    def __enter__(self):
        return iter(self.blocks)

    def __exit__(self, *args):
        return False


class FakeCH:
    def __init__(self, cluster="cluster", replicas=2, blocks=()):
        self.cluster = cluster
        self.replicas = replicas
        self.blocks = blocks
        self.inserts = []
        self.stream_query = ""

    def query(self, query):
        if "getMacro" in query:
            return QueryResult(self.cluster)
        if "clusterAllReplicas" in query and "system.one" in query:
            return QueryResult(self.replicas)
        raise AssertionError(query)

    def command(self, query):
        if "COUNT(1)" in query:
            return 1
        raise AssertionError(query)

    def query_row_block_stream(self, query):
        self.stream_query = query
        return FakeStream(self.blocks)

    def insert(self, table, rows, column_names):
        self.inserts.append((table, rows, column_names))


class DeleteError:
    def __init__(self, name):
        self.name = name


class FailingMinio:
    def remove_objects(self, bucket, objects):
        return iter([DeleteError("bad-object")])


@pytest.fixture
def args_factory():
    def make_args(**overrides):
        values = {
            "clustername": "cluster",
            "expected_replicas": 2,
            "dryrun_flag": False,
            "s3diskname": "s3",
            "useafter": None,
            "useage": 24,
            "usetotal": None,
            "samples": 1,
            "deletebatchsize": 1000,
            "order_by_objpath": False,
            "interactive_flag": False,
            "use_remove_objects": True,
            "s3bucket": "bucket",
            "keepdata_flag": True,
            "silent_flag": True,
            # S3 auth surface (static | aws | iam)
            "s3auth": "static",
            "s3profile": "",
            "s3accesskey": "",
            "s3secretkey": "",
            "s3sessiontoken": "",
            "s3region": "eu-central-1",
        }
        values.update(overrides)
        return types.SimpleNamespace(**values)

    return make_args


def test_preflight_rejects_wrong_cluster(s3gc_module, args_factory, monkeypatch):
    namespace = s3gc_module["preflight_cluster"].__globals__
    monkeypatch.setitem(namespace, "args", args_factory(clustername="expected"))
    monkeypatch.setitem(namespace, "ch_client", FakeCH(cluster="actual"))

    with pytest.raises(RuntimeError, match="cluster preflight failed"):
        s3gc_module["preflight_cluster"]()


def test_batch_errors_checkpoint_only_confirmed_deletes(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH(blocks=[[("good-object", 10, "time"), ("bad-object", 20, "time")]])
    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "ch_writer", client)
    monkeypatch.setitem(namespace, "minio_client", FailingMinio())

    with pytest.raises(s3gc_module["S3DeletionError"]):
        s3gc_module["do_use"]()

    assert client.inserts == [
        (
            "`s3objects_for_s3`",
            [["good-object", 10, "time", False]],
            ["objpath", "size", "last_modified", "active"],
        )
    ]


def test_delete_batches_are_checkpointed_independently(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH(blocks=[[("object-a", 10, "time"), ("object-b", 20, "time")]])

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory(deletebatchsize=1))
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "ch_writer", client)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())
    s3gc_module["do_use"]()

    assert [insert[1] for insert in client.inserts] == [
        [["object-a", 10, "time", False]],
        [["object-b", 20, "time", False]],
    ]


class SessionIsLocked(RuntimeError):
    """Stand-in for ClickHouse error 373."""


class StreamingCH(FakeCH):
    """A client that rejects writes while one of its result streams is open.

    Models ClickHouse's one-query-per-session rule. clickhouse-connect gives
    each client an auto-generated session_id, and `insert()` issues its own
    `DESCRIBE TABLE` before writing — a second concurrent query on the session
    the anti-join stream is still holding.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.streaming = False

    def query_row_block_stream(self, query):
        inner = super().query_row_block_stream(query)
        owner = self

        class Guarded:
            def __enter__(self):
                owner.streaming = True
                return inner.__enter__()

            def __exit__(self, *exc):
                owner.streaming = False
                return inner.__exit__(*exc)

        return Guarded()

    def insert(self, *args, **kwargs):
        if self.streaming:
            raise SessionIsLocked("Session is locked by a concurrent client")
        return super().insert(*args, **kwargs)


def test_tombstones_are_written_off_the_streaming_session(
    s3gc_module, args_factory, monkeypatch
):
    """Regression: the delete phase died on its first batch with SESSION_IS_LOCKED.

    Objects were already gone from S3 when the tombstone insert was rejected, so
    the deletion went unrecorded and the job could not resume cleanly. Tombstones
    must therefore be written on a client that is not holding the anti-join stream.
    """
    namespace = s3gc_module["do_use"].__globals__
    streaming = StreamingCH(blocks=[[("object-a", 10, "time")]])
    writer = FakeCH()

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_client", streaming)
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())

    s3gc_module["do_use"]()

    assert streaming.inserts == []
    assert [insert[1] for insert in writer.inserts] == [
        [["object-a", 10, "time", False]]
    ]


def test_connect_to_ch_builds_a_separate_writer_client(s3gc_module, monkeypatch):
    """The two clients must be distinct, or the session lock comes straight back."""
    namespace = s3gc_module["connect_to_ch"].__globals__
    built = []

    class FakeConnect:
        @staticmethod
        def get_client(**kwargs):
            client = object()
            built.append(client)
            return client

    monkeypatch.setitem(namespace, "clickhouse_connect", FakeConnect)
    monkeypatch.setitem(
        namespace,
        "args",
        types.SimpleNamespace(
            chhost="host",
            chport=8123,
            chuser="user",
            chpass="pass",
            chtimeout=60,
            s3path="path",
            s3bucket="bucket",
        ),
    )

    s3gc_module["connect_to_ch"]()

    assert len(built) == 2
    assert namespace["ch_client"] is not namespace["ch_writer"]


def test_delete_entrypoint_requires_confirmation():
    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "S3GC_PHASE": "delete",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert "Refusing delete" in result.stderr


def test_dev_automation_entrypoint_runs_collect_dry_run_and_delete(tmp_path):
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CALLS_PATH": str(calls_path),
            "S3GC_PHASE": "dev-automation",
            "S3GC_DELETE_CONFIRMATION": "DELETE_ORPHANS",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert calls_path.read_text().splitlines() == [
        "/app/s3gc.py --collectonly --keepdata --drop-collecttable",
        "/app/s3gc.py --usecollected --dry-run",
        "/app/s3gc.py --usecollected --keepdata --non-interactive",
    ]


def test_dev_automation_entrypoint_stops_after_an_error(tmp_path):
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
        "case \"$*\" in *--dry-run) exit 42 ;; esac\n"
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CALLS_PATH": str(calls_path),
            "S3GC_PHASE": "dev-automation",
            "S3GC_DELETE_CONFIRMATION": "DELETE_ORPHANS",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 42
    assert calls_path.read_text().splitlines() == [
        "/app/s3gc.py --collectonly --keepdata --drop-collecttable",
        "/app/s3gc.py --usecollected --dry-run",
    ]


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (("PHASE=dry-run", "PHASE=delete"), "delete requires"),
        (("PHASE=dry-run", "PHASE=dev-automation"), "dev-automation requires"),
        (("ORDER_BY_OBJPATH=false", "ORDER_BY_OBJPATH=yes"), "ORDER_BY_OBJPATH"),
    ],
)
def test_renderer_rejects_invalid_configuration(tmp_path, replacement, message):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "invalid.env"
    config_path.write_text(source.replace(*replacement))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert message in result.stderr


def test_renderer_accepts_confirmed_dev_automation(tmp_path):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "dev-automation.env"
    config_path.write_text(
        source.replace("PHASE=dry-run", "PHASE=dev-automation").replace(
            "DELETE_CONFIRMATION=", "DELETE_CONFIRMATION=DELETE_ORPHANS"
        )
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert 's3gc.altinity.com/phase: "dev-automation"' in result.stdout


def test_kubernetes_default_antijoin_does_not_globally_sort(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH()
    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True))
    monkeypatch.setitem(namespace, "ch_client", client)

    s3gc_module["do_use"]()

    assert "ORDER BY s3o.objpath" not in client.stream_query


def test_antijoin_ordering_is_an_explicit_opt_in(s3gc_module, args_factory, monkeypatch):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH()
    monkeypatch.setitem(
        namespace, "args", args_factory(dryrun_flag=True, order_by_objpath=True)
    )
    monkeypatch.setitem(namespace, "ch_client", client)

    s3gc_module["do_use"]()

    assert "ORDER BY s3o.objpath" in client.stream_query


def test_delete_transport_failure_reconnects_once(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["remove_objects_reconnecting"].__globals__
    attempts = []

    class TransportFailingMinio:
        def remove_objects(self, bucket, objects):
            attempts.append("failed")
            raise s3gc_module["urllib3"].exceptions.ReadTimeoutError(
                None, "https://s3.example", "timed out"
            )

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            attempts.append("success")
            return iter(())

    def reconnect():
        namespace["minio_client"] = SuccessfulMinio()

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "minio_client", TransportFailingMinio())
    monkeypatch.setitem(namespace, "connect_to_s3", reconnect)

    assert s3gc_module["remove_objects_reconnecting"]([("object-a", 10, "time")]) == []
    assert attempts == ["failed", "success"]


# ---------------------------------------------------------------------------
# Regression tests for defects found in production (SUP-30408).
# ---------------------------------------------------------------------------


def _load_with_env(monkeypatch, **env):
    """Load s3gc.py with the given S3GC_* environment, as a Kubernetes Job would."""
    import runpy

    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", [str(Path(__file__).resolve().parents[1] / "s3gc.py")])
    return runpy.run_path(
        str(Path(__file__).resolve().parents[1] / "s3gc.py"), run_name="s3gc_test"
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        ("false", False),
        ("False", False),
        ("no", False),
        ("off", False),
        ("0", False),
        ("", False),
        ("true", True),
        ("True", True),
        ("yes", True),
        ("on", True),
        ("1", True),
    ],
)
def test_coerce_bool_parses_env_spellings(s3gc_module, value, expected):
    assert s3gc_module["coerce_bool"](value) is expected


def test_coerce_bool_passes_through_real_bools_and_none(s3gc_module):
    assert s3gc_module["coerce_bool"](True) is True
    assert s3gc_module["coerce_bool"](False) is False
    assert s3gc_module["coerce_bool"](None) is False


def test_coerce_bool_rejects_nonsense(s3gc_module):
    with pytest.raises(ValueError):
        s3gc_module["coerce_bool"]("maybe")


@pytest.mark.parametrize(
    "dest, env_name",
    [
        ("s3secure_flag", "S3GC_S3SECURE_FLAG"),
        ("dryrun_flag", "S3GC_DRYRUN_FLAG"),
        ("keepdata_flag", "S3GC_KEEPDATA_FLAG"),
        ("order_by_objpath", "S3GC_ORDER_BY_OBJPATH"),
        ("verbose_flag", "S3GC_VERBOSE_FLAG"),
    ],
)
def test_boolean_env_false_is_false(monkeypatch, dest, env_name):
    """S3GC_*=false must not remain the truthy string 'false'."""
    module = _load_with_env(monkeypatch, **{env_name: "false"})
    assert getattr(module["args"], dest) is False


def test_bare_cli_flag_still_enables(monkeypatch):
    """Coercion must not break `--dryrun` used as a bare flag."""
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", [str(root / "s3gc.py"), "--dryrun"])
    module = runpy.run_path(str(root / "s3gc.py"), run_name="s3gc_test")
    assert module["args"].dryrun_flag is True


def test_removed_s3useiam_cli_flag_is_rejected(monkeypatch):
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", [str(root / "s3gc.py"), "--s3useiam"])
    with pytest.raises(ArgumentError, match="Unrecognized arguments: --s3useiam"):
        runpy.run_path(str(root / "s3gc.py"), run_name="s3gc_test")


def test_collect_age_filter_uses_total_seconds(s3gc_module, args_factory, monkeypatch):
    """--age 24 must keep a 30-day-old object.

    The filter used timedelta.seconds (the sub-day remainder, 0..86399), so
    computed age never exceeded 23 h and --age 24 collected nothing at all,
    leaving an empty aux table and a dry-run that reported a clean bucket.
    """
    import datetime

    namespace = s3gc_module["do_collect"].__globals__
    now = datetime.datetime.now(datetime.timezone.utc)

    class Obj:
        def __init__(self, name, age):
            self.object_name = name
            self.size = 1
            self.last_modified = now - age

    old = Obj("thirty-days-old", datetime.timedelta(days=30, hours=5))
    fresh = Obj("one-hour-old", datetime.timedelta(hours=1))

    class Minio:
        def list_objects(self, bucket, prefix, recursive, start_after):
            return iter([old, fresh])

    class CH:
        def __init__(self):
            self.rows = []

        def command(self, query):
            return None

        def insert(self, table, rows, column_names):
            self.rows.extend(rows)

    ch = CH()
    monkeypatch.setitem(
        namespace,
        "args",
        args_factory(
            age=24,
            collectbatchsize=10,
            total=None,
            collectafter="",
            s3path="",
            s3bucket="bucket",
            createdatabase_flag=False,
            drop_collecttable_flag=False,
        ),
    )
    monkeypatch.setitem(namespace, "minio_client", Minio())
    monkeypatch.setitem(namespace, "ch_client", ch)
    monkeypatch.setitem(namespace, "tname", "`aux`")

    s3gc_module["do_collect"]()

    collected = [row[0] for row in ch.rows]
    assert "thirty-days-old" in collected
    assert "one-hour-old" not in collected


def test_usecollected_without_aux_table_fails_loudly(
    s3gc_module, args_factory, monkeypatch
):
    """An absent/empty aux table used to exit 0 — indistinguishable from success.

    That is exactly what a load-balanced CHHOST produces, because the aux table
    is a node-local ReplacingMergeTree.
    """
    namespace = s3gc_module["do_use"].__globals__

    class EmptyCH(FakeCH):
        def command(self, query):
            return 0

    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True, chhost="replica-1"))
    monkeypatch.setitem(namespace, "ch_client", EmptyCH())
    monkeypatch.setitem(namespace, "tname", "`aux`")

    with pytest.raises(RuntimeError, match="does not exist or is empty"):
        s3gc_module["do_use"]()


def test_samples_mismatch_warns(s3gc_module, args_factory, monkeypatch, caplog):
    """--samples must match the aux table's PARTITION BY or pruning is lost."""
    namespace = s3gc_module["check_samples_match_partitioning"].__globals__

    class PartitionedCH:
        def query(self, query):
            return QueryResult("CRC32(objpath) % 4")

    monkeypatch.setitem(namespace, "args", args_factory(samples=3))
    monkeypatch.setitem(namespace, "ch_client", PartitionedCH())
    monkeypatch.setitem(namespace, "tname", "`aux`")

    with caplog.at_level("WARNING"):
        s3gc_module["check_samples_match_partitioning"]()

    assert "does not match" in caplog.text


def test_gcs_endpoint_disables_batch_delete(s3gc_module, args_factory, monkeypatch):
    """GCS has no batch DeleteObjects; remove_objects() fails there."""
    namespace = s3gc_module["connect_to_s3"].__globals__
    parsed = args_factory(
        s3ip="storage.googleapis.com",
        s3port=443,
        use_remove_objects=True,
        s3secure_flag=True,
        s3accesskey="k",
        s3secretkey="s",
        s3region="auto",
        s3sslcertfile="",
        s3_connect_timeout=15,
        s3_read_timeout=120,
        s3_retries=3,
    )
    monkeypatch.setitem(namespace, "args", parsed)
    monkeypatch.setitem(namespace, "Minio", lambda *a, **k: object())

    s3gc_module["connect_to_s3"]()

    assert parsed.use_remove_objects is False


def test_renderer_omits_empty_image_pull_secret(tmp_path):
    """A public image needs no pull secret; `- name: ""` would be meaningless."""
    config_path = tmp_path / "public.env"
    config_path.write_text((ROOT / "deploy/kubernetes/example.env").read_text())

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "imagePullSecrets" not in result.stdout
    assert 'name: ""' not in result.stdout


def test_renderer_omits_unset_usetotal(tmp_path):
    """An empty USETOTAL must render no variable at all.

    s3gc parses S3GC_USETOTAL as an integer, so `value: ""` would fail the run
    at startup rather than mean "no limit".
    """
    config_path = tmp_path / "full.env"
    config_path.write_text((ROOT / "deploy/kubernetes/example.env").read_text())

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "S3GC_USETOTAL" not in result.stdout


def test_renderer_keeps_configured_usetotal(tmp_path):
    """A bounded trial run must reach the container."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "bounded.env"
    config_path.write_text(source.replace("USETOTAL=", "USETOTAL=5000"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "S3GC_USETOTAL" in result.stdout
    assert '"5000"' in result.stdout


@pytest.mark.parametrize("value", ["0", "-1", "all", "1.5"])
def test_renderer_rejects_invalid_usetotal(tmp_path, value):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "bad.env"
    config_path.write_text(source.replace("USETOTAL=", f"USETOTAL={value}"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert "USETOTAL must be a positive integer when set" in result.stderr


def test_renderer_accepts_an_env_file_without_usetotal(tmp_path):
    """USETOTAL is optional: pre-existing env files must keep rendering."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    without = "\n".join(
        line for line in source.splitlines() if not line.startswith("USETOTAL=")
    )
    config_path = tmp_path / "legacy.env"
    config_path.write_text(without)

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "S3GC_USETOTAL" not in result.stdout


def test_renderer_keeps_configured_image_pull_secret(tmp_path):
    """A private mirror must still be able to set one."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "private.env"
    config_path.write_text(
        source.replace("IMAGE_PULL_SECRET=", "IMAGE_PULL_SECRET=my-mirror-pull")
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "imagePullSecrets:" in result.stdout
    assert "- name: \"my-mirror-pull\"" in result.stdout


# ---------------------------------------------------------------------------
# S3 authentication modes (merged from Altinity/s3gc PR #2, unified with iam).
# ---------------------------------------------------------------------------


def _resolve(s3gc_module, monkeypatch, **overrides):
    namespace = s3gc_module["resolve_s3_credentials"].__globals__
    args = types.SimpleNamespace(
        s3auth="static", s3profile="",
        s3accesskey="", s3secretkey="", s3sessiontoken="", s3region="eu-central-1",
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    monkeypatch.setitem(namespace, "args", args)
    return s3gc_module["resolve_s3_credentials"]()


def test_static_mode_returns_supplied_keys(s3gc_module, monkeypatch):
    result = _resolve(s3gc_module, monkeypatch, s3accesskey="AK", s3secretkey="SK")
    assert result[0] == "AK" and result[1] == "SK"
    assert result[2] is None          # no session token
    assert result[4] == "static"


def test_static_mode_carries_session_token(s3gc_module, monkeypatch):
    result = _resolve(
        s3gc_module, monkeypatch, s3accesskey="AK", s3secretkey="SK", s3sessiontoken="TOKEN"
    )
    assert result[2] == "TOKEN"


def test_static_mode_without_keys_is_anonymous(s3gc_module, monkeypatch):
    assert _resolve(s3gc_module, monkeypatch)[4] == "anonymous"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"s3accesskey": "AK"}, "must be specified together"),
        ({"s3secretkey": "SK"}, "must be specified together"),
        ({"s3sessiontoken": "TOKEN"}, "requires s3accesskey"),
    ],
)
def test_static_mode_rejects_incomplete_credentials(s3gc_module, monkeypatch, overrides, message):
    with pytest.raises(ValueError, match=message):
        _resolve(s3gc_module, monkeypatch, **overrides)


def test_iam_mode_defers_to_the_provider(s3gc_module, monkeypatch):
    """iam returns no keys: MinIO gets the provider so it can refresh them."""
    result = _resolve(s3gc_module, monkeypatch, s3auth="iam")
    assert result[:3] == (None, None, None)
    assert result[4] == "iam"


def test_s3profile_implies_aws_mode(s3gc_module, monkeypatch):
    calls = []
    namespace = s3gc_module["resolve_s3_credentials"].__globals__
    monkeypatch.setitem(
        namespace, "resolve_aws_s3_credentials",
        lambda: calls.append("aws") or (None, None, None, "eu-central-1", "aws"),
    )
    assert _resolve(s3gc_module, monkeypatch, s3profile="sso")[4] == "aws"
    assert calls == ["aws"]


def test_unknown_auth_mode_is_rejected(s3gc_module, monkeypatch):
    with pytest.raises(ValueError, match="s3auth must be one of"):
        _resolve(s3gc_module, monkeypatch, s3auth="magic")


@pytest.mark.parametrize(
    "overrides",
    [
        {"s3auth": "iam", "s3profile": "sso"},      # profile implies aws, conflicts with iam
    ],
)
def test_contradictory_auth_settings_error(s3gc_module, monkeypatch, overrides):
    """A contradiction must fail, not silently pick a winner and send the wrong identity."""
    with pytest.raises(ValueError, match="conflicts with"):
        _resolve(s3gc_module, monkeypatch, **overrides)


def test_aws_mode_rejects_explicit_keys(s3gc_module, monkeypatch):
    with pytest.raises(ValueError, match="cannot be combined with explicit"):
        _resolve(s3gc_module, monkeypatch, s3auth="aws", s3accesskey="AK", s3secretkey="SK")


def test_aws_mode_without_boto3_is_user_visible(s3gc_module, monkeypatch):
    """A missing optional dependency must not surface as a bare ImportError."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *rest):
        if name == "boto3":
            raise ImportError("no boto3")
        return real_import(name, *rest)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(s3gc_module["UserVisibleError"], match="boto3 is required"):
        _resolve(s3gc_module, monkeypatch, s3auth="aws")


def test_format_s3_list_error_names_the_permission(s3gc_module, args_factory, monkeypatch):
    """The listing failure must tell the operator exactly what to grant."""
    namespace = s3gc_module["format_s3_list_error"].__globals__
    monkeypatch.setitem(
        namespace, "args", args_factory(s3bucket="my-bucket", s3path="pre/fix/", s3profile="sso")
    )

    class Err:
        code = "AccessDenied"
        message = "denied"

    text = s3gc_module["format_s3_list_error"](Err())
    assert "s3:ListBucket" in text
    assert "my-bucket" in text and "pre/fix/" in text
    assert "even with --dry-run" in text
    assert "--profile sso" in text


def test_iam_mode_keeps_the_hardened_transport(s3gc_module, args_factory, monkeypatch):
    """The merge must not revert to a bare PoolManager: that hung a run for 2h19m."""
    namespace = s3gc_module["connect_to_s3"].__globals__
    captured = {}
    monkeypatch.setitem(namespace, "Minio", lambda endpoint, **kw: captured.update(kw) or object())
    monkeypatch.setitem(
        namespace, "args",
        args_factory(s3auth="iam", s3ip="s3.eu-central-1.amazonaws.com", s3port=443,
                     s3secure_flag=True, s3sslcertfile="", s3_connect_timeout=15,
                     s3_read_timeout=120, s3_retries=3),
    )
    s3gc_module["connect_to_s3"]()

    assert "credentials" in captured            # provider, not frozen keys
    assert "access_key" not in captured
    timeout = captured["http_client"].connection_pool_kw["timeout"]
    assert timeout.read_timeout == 120 and timeout.connect_timeout == 15


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (("S3AUTH=iam", "S3AUTH=magic"), "S3AUTH must be static, aws or iam"),
        (("S3PROFILE=", "S3PROFILE=sso"), "S3PROFILE requires S3AUTH=aws"),
    ],
)
def test_renderer_validates_auth_configuration(tmp_path, replacement, message):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "auth.env"
    config_path.write_text(source.replace(*replacement))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 64
    assert message in result.stderr


def test_renderer_wires_auth_env_into_the_job(tmp_path):
    """PR #2's flags were unreachable from a Job until the template exposed them."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "aws.env"
    config_path.write_text(source.replace("S3AUTH=iam", "S3AUTH=aws").replace("S3PROFILE=", "S3PROFILE=sso"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0
    assert 'name: S3GC_S3AUTH' in result.stdout
    assert 'value: "aws"' in result.stdout
    assert 'value: "sso"' in result.stdout
