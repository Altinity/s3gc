import os
import runpy
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_s3gc():
    original_argv = sys.argv[:]
    try:
        sys.argv = [str(ROOT / "s3gc.py")]
        return runpy.run_path(str(ROOT / "s3gc.py"), run_name="s3gc_test")
    finally:
        sys.argv = original_argv


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
        return FakeStream(self.blocks)

    def insert(self, table, rows, column_names):
        self.inserts.append((table, rows, column_names))


class DeleteError:
    def __init__(self, name):
        self.name = name


class FakeMinio:
    def remove_objects(self, bucket, objects):
        return iter([DeleteError("bad-object")])


def use_args(**overrides):
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
        "interactive_flag": False,
        "use_remove_objects": True,
        "s3bucket": "bucket",
        "keepdata_flag": True,
        "silent_flag": True,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class S3GCTest(unittest.TestCase):
    def test_preflight_rejects_wrong_cluster(self):
        module = load_s3gc()
        namespace = module["preflight_cluster"].__globals__
        namespace["args"] = use_args(clustername="expected")
        namespace["ch_client"] = FakeCH(cluster="actual")

        with self.assertRaisesRegex(RuntimeError, "cluster preflight failed"):
            module["preflight_cluster"]()

    def test_batch_errors_checkpoint_only_confirmed_deletes(self):
        module = load_s3gc()
        namespace = module["do_use"].__globals__
        namespace["args"] = use_args()
        namespace["ch_client"] = FakeCH(
            blocks=[[("good-object", 10, "time"), ("bad-object", 20, "time")]]
        )
        namespace["minio_client"] = FakeMinio()

        with self.assertRaises(module["S3DeletionError"]):
            module["do_use"]()

        self.assertEqual(len(namespace["ch_client"].inserts), 1)
        self.assertEqual(
            namespace["ch_client"].inserts[0][1], [["good-object", 10, "time", False]]
        )

    def test_delete_batches_are_checkpointed_independently(self):
        module = load_s3gc()
        namespace = module["do_use"].__globals__
        namespace["args"] = use_args(deletebatchsize=1)
        namespace["ch_client"] = FakeCH(
            blocks=[[("object-a", 10, "time"), ("object-b", 20, "time")]]
        )

        class SuccessfulMinio:
            def remove_objects(self, bucket, objects):
                return iter(())

        namespace["minio_client"] = SuccessfulMinio()
        module["do_use"]()

        self.assertEqual(len(namespace["ch_client"].inserts), 2)
        self.assertEqual(namespace["ch_client"].inserts[0][1], [["object-a", 10, "time", False]])
        self.assertEqual(namespace["ch_client"].inserts[1][1], [["object-b", 20, "time", False]])

    def test_delete_entrypoint_requires_confirmation(self):
        result = subprocess.run(
            ["sh", str(ROOT / "kubernetes-entrypoint.sh")],
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
        self.assertEqual(result.returncode, 64)
        self.assertIn("Refusing delete", result.stderr)

    def test_render_rejects_unacknowledged_delete(self):
        source = (ROOT / "deploy/kubernetes/example.env").read_text()
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as config:
            config.write(source.replace("PHASE=dry-run", "PHASE=delete"))
            config_path = config.name
        self.addCleanup(lambda: os.unlink(config_path))

        result = subprocess.run(
            [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 64)
        self.assertIn("delete requires", result.stderr)


if __name__ == "__main__":
    unittest.main()
