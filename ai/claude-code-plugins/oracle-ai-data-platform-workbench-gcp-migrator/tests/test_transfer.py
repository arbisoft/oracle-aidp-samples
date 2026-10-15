"""`transfer`: Cloud Storage buckets → OCI, a dry run until --apply (rule GS03_TRANSFER)."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.transfer import TransferError, transfer
from tests.test_export import FakeGoogle, FakeOci, md5


def spec(gcs: str, oci: str, ns: str = "ns") -> dict:
    return {"gcs_bucket": gcs, "oci_bucket": oci, "namespace": ns, "gcs": f"gs://{gcs}/", "oci": f"oci://{oci}@{ns}/"}


class Transfer(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        (self.out / "transfers").mkdir()
        for s in (spec("landing", "landing-oci"), spec("scratch", "scratch")):
            (self.out / "transfers" / f"{s['gcs_bucket']}.transfer.json").write_text(json.dumps(s))
        self.google, self.oci = FakeGoogle(), FakeOci()
        self.google.objects.update({"landing/ext/a.parquet": b"aaaa", "landing/ext/b.csv": b"bb",
                                    "landing/ext/": b"", "scratch/tmp/x": b"x"})
        self.oci.objects["ext/a.parquet"] = (b"aaaa", md5(b"aaaa"))  # copied by an earlier run
        self.lines: list[str] = []

    def run_transfer(self, **kw):
        return transfer(self.out, reader=self.google, oci_client=self.oci, log=self.lines.append, **kw)

    def test_dry_run_counts_and_sends_nothing(self):
        result = self.run_transfer()
        self.assertEqual(self.oci.puts, [])
        self.assertNotIn("report", result)
        landing = result["buckets"]["landing"]
        # the console "folder" ext/ is not a file; a.parquet is already there with its MD5
        self.assertEqual((landing["files"], landing["to_copy"], landing["already_in_oci"]), (2, 1, 1))
        self.assertIn("would copy", self.lines[0])

    def test_apply_copies_what_is_missing_and_resumes(self):
        result = self.run_transfer(apply=True, buckets=["landing"])
        self.assertEqual(self.oci.puts, ["ext/b.csv"])
        self.assertEqual(list(result["buckets"]), ["landing"])  # --bucket: only that one
        self.assertTrue(Path(result["report"]).exists())
        self.oci.puts.clear()
        self.run_transfer(apply=True, buckets=["landing"])
        self.assertEqual(self.oci.puts, [])

    def test_a_composite_object_is_checked_by_size(self):
        class Composite(FakeGoogle):
            def get(self, url, params=None, timeout=None, stream=False):
                r = super().get(url, params, timeout, stream)
                if r._body:
                    for o in r._body["items"]:
                        o.pop("md5Hash")
                return r
        self.google.__class__ = Composite
        first = self.run_transfer(apply=True, buckets=["landing"])["buckets"]["landing"]
        self.assertEqual(first["checked_by_size"], 2)
        self.oci.puts.clear()
        self.run_transfer(apply=True, buckets=["landing"])
        self.assertEqual(self.oci.puts, [])  # same sizes in OCI: nothing copied again

    def test_one_bucket_failing_does_not_stop_the_others(self):
        class MissingBucket(FakeOci):
            def list_objects(self, namespace, bucket, **kw):
                if bucket == "landing-oci":
                    raise RuntimeError("BucketNotFound")
                return super().list_objects(namespace, bucket, **kw)
        self.oci.__class__ = MissingBucket
        result = self.run_transfer(apply=True)
        self.assertIn("BucketNotFound", result["buckets"]["landing"]["error"])
        self.assertEqual(result["buckets"]["scratch"]["copied"], 1)

    def test_refuses_an_unknown_bucket_or_no_namespace(self):
        with self.assertRaisesRegex(TransferError, "not in this migration: nope"):
            self.run_transfer(buckets=["nope"])
        (self.out / "transfers" / "scratch.transfer.json").write_text(json.dumps(spec("scratch", "scratch", ns="")))
        with self.assertRaisesRegex(TransferError, "no OCI namespace for scratch"):
            self.run_transfer()

    def test_needs_a_finished_migration_with_buckets(self):
        with tempfile.TemporaryDirectory() as empty, self.assertRaisesRegex(TransferError, "no Cloud Storage bucket"):
            transfer(empty, reader=self.google, oci_client=self.oci)


if __name__ == "__main__":
    unittest.main()
