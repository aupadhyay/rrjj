import tempfile
import unittest
from pathlib import Path

from bench.paired_runner import BenchmarkDataError
from bench.s3_smoke import (
    DEFAULT_S3_PREFIX,
    download_session,
    exact_session_prefix,
    hash_tree,
    normalize_prefix,
)


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.paginator = FakePaginator(
            [
                {
                    "Contents": [
                        {"Key": key, "Size": len(value)}
                        for key, value in objects.items()
                    ]
                }
            ]
        )

    def get_paginator(self, operation_name):
        if operation_name != "list_objects_v2":
            raise AssertionError(operation_name)
        return self.paginator

    def download_file(self, bucket, key, filename):
        Path(filename).write_bytes(self.objects[key])


class PrefixTest(unittest.TestCase):
    def test_uses_public_default_prefix(self):
        self.assertEqual(DEFAULT_S3_PREFIX, "rrjj/modal")

    def test_normalizes_prefix_and_appends_session(self):
        self.assertEqual(normalize_prefix("/rrjj/dev/"), "rrjj/dev")
        self.assertEqual(
            exact_session_prefix("/rrjj/dev/", "s3-smoke-id"),
            "rrjj/dev/s3-smoke-id",
        )

    def test_rejects_ambiguous_or_unsafe_prefixes(self):
        for prefix in ("", "/", "rrjj//dev", "rrjj/../dev", "rrjj/./dev"):
            with self.subTest(prefix=prefix):
                with self.assertRaises(ValueError):
                    normalize_prefix(prefix)


class DownloadTest(unittest.TestCase):
    def test_downloads_only_exact_session_prefix_and_reports_totals(self):
        prefix = "rrjj/dev/session"
        objects = {
            f"{prefix}/manifest.json": b"{}",
            f"{prefix}/events/0001.ndjson": b"event\n",
            f"{prefix}/store/repo/object": b"store",
        }
        client = FakeS3(objects)
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "session"
            metrics = download_session(client, "bucket", prefix, destination)
            self.assertEqual(metrics["object_count"], 3)
            self.assertEqual(
                metrics["object_bytes"], sum(map(len, objects.values()))
            )
            self.assertEqual(
                (destination / "store/repo/object").read_bytes(), b"store"
            )
            self.assertEqual(
                client.paginator.calls,
                [{"Bucket": "bucket", "Prefix": f"{prefix}/"}],
            )

    def test_rejects_keys_outside_prefix(self):
        client = FakeS3({"other/session/manifest.json": b"{}"})
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkDataError, "outside"):
                download_session(
                    client,
                    "bucket",
                    "rrjj/dev/session",
                    Path(temporary) / "session",
                )


class HashTreeTest(unittest.TestCase):
    def test_hashes_file_contents_by_relative_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "nested").mkdir()
            (root / "nested/file").write_bytes(b"deterministic")
            hashes = hash_tree(root)
            self.assertEqual(list(hashes), ["nested/file"])
            self.assertEqual(len(hashes["nested/file"]), 64)


if __name__ == "__main__":
    unittest.main()
