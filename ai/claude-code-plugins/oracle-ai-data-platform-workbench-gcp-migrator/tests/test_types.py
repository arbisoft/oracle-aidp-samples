"""One test per type rule (references/type-mapping.md)."""
from __future__ import annotations

import unittest

from gcp_aidp.translate.types import map_column, map_type_name


def m(type_, mode="NULLABLE", **kw):
    return map_column({"name": "c", "type": type_, "mode": mode, **kw})


class TypeRules(unittest.TestCase):
    def check(self, col, target, rule, severity):
        self.assertEqual((col["target_type"], col["severity"]), (target, severity))
        self.assertIn(rule, col["rules"])

    def test_TY01_INT64(self):
        self.check(m("INTEGER"), "BIGINT", "TY01_INT64", "map")
        self.check(m("INT64"), "BIGINT", "TY01_INT64", "map")

    def test_TY02_FLOAT64(self):
        self.check(m("FLOAT"), "DOUBLE", "TY02_FLOAT64", "map")

    def test_TY03_BOOL(self):
        self.check(m("BOOLEAN"), "BOOLEAN", "TY03_BOOL", "map")

    def test_TY04_STRING(self):
        self.check(m("STRING"), "STRING", "TY04_STRING", "map")

    def test_TY05_BYTES(self):
        self.check(m("BYTES"), "BINARY", "TY05_BYTES", "map")

    def test_TY06_DATE(self):
        self.check(m("DATE"), "DATE", "TY06_DATE", "map")

    def test_TY07_TIMESTAMP(self):
        self.check(m("TIMESTAMP"), "TIMESTAMP", "TY07_TIMESTAMP", "map")

    def test_TY08_NUMERIC(self):
        self.check(m("NUMERIC"), "DECIMAL(38,9)", "TY08_NUMERIC", "map")
        self.check(m("NUMERIC", precision="10", scale="2"), "DECIMAL(10,2)", "TY08_NUMERIC", "map")

    def test_TY09_BIGNUMERIC(self):
        self.check(m("BIGNUMERIC"), None, "TY09_BIGNUMERIC", "block")
        self.check(m("BIGNUMERIC", precision="30", scale="10"), "DECIMAL(30,10)", "TY09_BIGNUMERIC", "map")
        string = map_column({"name": "c", "type": "BIGNUMERIC"}, bignumeric="string")
        self.check(string, "STRING", "TY09_BIGNUMERIC", "caveat")

    def test_TY10_DATETIME(self):
        col = m("DATETIME")
        self.check(col, "TIMESTAMP", "TY10_DATETIME", "caveat")
        self.assertIn("UTC", col["details"][0])

    def test_TY11_TIME(self):
        self.check(m("TIME"), "STRING", "TY11_TIME", "flag")

    def test_TY12_STRUCT(self):
        col = m("RECORD", fields=[{"name": "a", "type": "INTEGER"}, {"name": "b", "type": "JSON"}])
        self.check(col, "STRUCT<`a`: BIGINT, `b`: STRING>", "TY12_STRUCT", "caveat")
        self.assertIn("TY14_JSON", col["rules"])
        blocked = m("RECORD", fields=[{"name": "g", "type": "GEOGRAPHY"}])
        self.check(blocked, None, "TY12_STRUCT", "block")

    def test_TY13_ARRAY(self):
        self.check(m("STRING", "REPEATED"), "ARRAY<STRING>", "TY13_ARRAY", "map")
        nested = m("RECORD", "REPEATED", fields=[{"name": "k", "type": "STRING"}])
        self.check(nested, "ARRAY<STRUCT<`k`: STRING>>", "TY13_ARRAY", "map")

    def test_TY14_JSON(self):
        self.check(m("JSON"), "STRING", "TY14_JSON", "caveat")

    def test_TY15_GEOGRAPHY(self):
        self.check(m("GEOGRAPHY"), None, "TY15_GEOGRAPHY", "block")
        wkt = map_column({"name": "c", "type": "GEOGRAPHY"}, geography="wkt")
        self.check(wkt, "STRING", "TY15_GEOGRAPHY", "caveat")

    def test_TY16_INTERVAL(self):
        self.check(m("INTERVAL"), None, "TY16_INTERVAL", "block")

    def test_TY17_RANGE(self):
        self.check(m("RANGE"), None, "TY17_RANGE", "block")

    def test_TY99_UNKNOWN(self):
        self.check(m("VECTOR"), None, "TY99_UNKNOWN", "block")

    def test_required_is_not_nullable(self):
        self.assertFalse(m("STRING", "REQUIRED")["nullable"])
        self.assertTrue(m("STRING")["nullable"])

    def test_type_names_in_sql(self):
        self.assertEqual(map_type_name("FLOAT64")[0], "DOUBLE")
        self.assertEqual(map_type_name("BOOL")[0], "BOOLEAN")


if __name__ == "__main__":
    unittest.main()
