"""DDL layout rules (D0x) and gs:// rules (GS0x)."""
from __future__ import annotations

import unittest

from gcp_aidp.translate import ddl
from gcp_aidp.translate.gcs_to_oci import build_transfer, location_for, rewrite_text, rewrite_uri
from gcp_aidp.translate.types import map_column

T = {"catalog": "c", "schema": "s", "name": "t"}


def layout(columns, **table):
    cols = [map_column({"name": n, "type": ty}) for n, ty in columns]
    return ddl.table_layout(table, cols)


def rule_set(lay):
    return {(r, sev) for r, sev, _ in lay["findings"]}


class Layout(unittest.TestCase):
    def test_D01_PARTITION(self):
        lay = layout([("d", "DATE")], partitioning={"type": "DAY", "field": "d"})
        self.assertEqual((lay["partitioned_by"], lay["cluster_by"]), (["d"], []))
        self.assertIn(("D01_PARTITION", "rewrite"), rule_set(lay))

    def test_D02_CLUSTER_partition_plus_clustering(self):
        lay = layout([("d", "DATE"), ("a", "STRING"), ("b", "INTEGER")],
                     partitioning={"type": "DAY", "field": "d"}, clustering=["a", "b"])
        self.assertEqual((lay["partitioned_by"], lay["cluster_by"]), ([], ["d", "a", "b"]))

    def test_D02_CLUSTER_non_exact_partitioning(self):
        for part, ty in (({"type": "MONTH", "field": "d"}, "DATE"), ({"type": "DAY", "field": "d"}, "TIMESTAMP"),
                         ({"type": "RANGE", "field": "d", "range": {}}, "INTEGER")):
            lay = layout([("d", ty)], partitioning=part)
            self.assertEqual((lay["partitioned_by"], lay["cluster_by"]), ([], ["d"]), part)

    def test_D02_CLUSTER_limits(self):
        cols = [(f"k{i}", "STRING") for i in range(5)] + [("blob", "BYTES")]
        lay = layout(cols, clustering=[c for c, _ in cols])
        self.assertEqual(lay["cluster_by"], ["k0", "k1", "k2", "k3"])
        details = " ".join(d for _, _, d in lay["findings"])
        self.assertIn("at most 4", details)
        self.assertIn("blob not clustered", details)

    def test_D02_CLUSTER_keys_written_bare(self):
        # AIDP keeps a quoted key's backticks in its name; only plain names cluster.
        lay = layout([("a", "STRING"), ("we ird", "STRING")], clustering=["a", "we ird"])
        self.assertEqual(lay["cluster_by"], ["a"])
        sql = ddl.create_table(T, [{"name": "a", "target_type": "STRING"}], cluster_by=lay["cluster_by"])
        self.assertTrue(sql.endswith("CLUSTER BY (a)"), sql)

    def test_D03_INGESTION_TIME(self):
        lay = layout([("a", "STRING")], partitioning={"type": "DAY", "field": None})
        self.assertIn(("D03_INGESTION_TIME", "flag"), rule_set(lay))
        self.assertEqual((lay["partitioned_by"], lay["cluster_by"]), ([], []))

    def test_D04_METADATA(self):
        lay = layout([("a", "STRING")], expiration_ms=1, labels={"x": "y"})
        self.assertEqual([r for r, _, _ in lay["findings"]], ["D04_METADATA", "D04_METADATA"])

    def test_D05_EXTERNAL_FORMAT(self):
        sql, f = ddl.create_external_table(T, "PARQUET", "oci://b@n/p/")
        self.assertIn("USING PARQUET\nLOCATION 'oci://b@n/p/'", sql)
        self.assertNotIn("flag", [s for _, s, _ in f])
        _, csv = ddl.create_external_table(T, "CSV", "oci://b@n/p/")
        self.assertIn("flag", [s for _, s, _ in csv])
        self.assertIsNone(ddl.create_external_table(T, "GOOGLE_SHEETS", "x")[0])

    def test_D06_SQL_FUNCTION(self):
        sql, f = ddl.create_function(T, [{"name": "a", "type": "FLOAT64"}], "FLOAT64", "a * 2")
        self.assertIn("(a DOUBLE)\nRETURNS DOUBLE\nRETURN a * 2", sql)
        self.assertEqual(f[0][:2], ("D06_SQL_FUNCTION", "flag"))

    def test_create_table_quotes_and_escapes(self):
        cols = [{"name": "we`ird", "target_type": "STRING", "nullable": False, "comment": "it's \\ ok"}]
        sql = ddl.create_table(T, cols, comment="o'k")
        self.assertIn("`we``ird` STRING NOT NULL COMMENT 'it\\'s \\\\ ok'", sql)
        self.assertTrue(sql.startswith("CREATE TABLE IF NOT EXISTS `c`.`s`.`t`"))
        self.assertIn("COMMENT 'o\\'k'", sql)

    def test_M01_MATERIALIZED_VIEW(self):
        snap, refresh = ddl.materialized_view(T, "SELECT 1")
        self.assertTrue(snap.startswith("CREATE TABLE IF NOT EXISTS `c`.`s`.`t` USING DELTA AS"))
        self.assertTrue(refresh.startswith("INSERT OVERWRITE TABLE `c`.`s`.`t`"))


class Gcs(unittest.TestCase):
    def test_GS01_BUCKET_MAP(self):
        buckets = {"raw-data": "raw-data-oci"}
        self.assertEqual(rewrite_uri("gs://raw-data/x/y.csv", buckets, "ns"), "oci://raw-data-oci@ns/x/y.csv")
        self.assertIsNone(rewrite_uri("gs://other/x", buckets, "ns"))
        text, done, missing = rewrite_text("read gs://raw-data/p and gs://other/q", buckets, "ns")
        self.assertEqual((text, done, missing),
                         ("read oci://raw-data-oci@ns/p and gs://other/q", ["gs://raw-data/p"], ["gs://other/q"]))

    def test_GS02_LOCATION(self):
        self.assertEqual(location_for(["gs://raw-data/p/*.parquet"])[0], "gs://raw-data/p/")
        self.assertEqual(location_for(["gs://a/p/file.csv"]), ("gs://a/p/file.csv", ""))
        self.assertIsNone(location_for(["gs://a/*/x.csv"])[0])
        self.assertIsNone(location_for(["gs://a/1", "gs://a/2"])[0])

    def test_GS03_TRANSFER(self):
        script = build_transfer("src", "dst", "ns")
        self.assertIn('rclone --config "$CONF" copy "gcssrc:src" "ocidest:dst"', script)
        commands = [l for l in script.splitlines() if l.startswith("rclone")]
        self.assertEqual([c.split()[3] for c in commands], ["mkdir", "copy"])  # never sync/delete/purge


if __name__ == "__main__":
    unittest.main()
