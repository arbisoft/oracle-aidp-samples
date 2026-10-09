"""BigQuery column type → Delta column type, one named rule per decision.

Severity, worst last:

  map     exact
  caveat  carried, with a stated condition (recorded on the column)
  flag    carried, but a human must review it
  block   not carried: the table is not created

Accepts both the `tables.get` API names (INTEGER, FLOAT, BOOLEAN, RECORD) and
the GoogleSQL names (INT64, FLOAT64, BOOL, STRUCT). The authoritative table is
references/type-mapping.md; every rule there has a test.
"""
from __future__ import annotations

SEVERITY = ("map", "caveat", "flag", "block")

_SIMPLE = {  # source name: (Delta type, rule)
    "INTEGER": ("BIGINT", "TY01_INT64"), "INT64": ("BIGINT", "TY01_INT64"),
    "FLOAT": ("DOUBLE", "TY02_FLOAT64"), "FLOAT64": ("DOUBLE", "TY02_FLOAT64"),
    "BOOLEAN": ("BOOLEAN", "TY03_BOOL"), "BOOL": ("BOOLEAN", "TY03_BOOL"),
    "STRING": ("STRING", "TY04_STRING"),
    "BYTES": ("BINARY", "TY05_BYTES"),
    "DATE": ("DATE", "TY06_DATE"),
    "TIMESTAMP": ("TIMESTAMP", "TY07_TIMESTAMP"),
}
_BLOCKED = {
    "INTERVAL": ("TY16_INTERVAL", "no Delta INTERVAL column type"),
    "RANGE": ("TY17_RANGE", "no Delta RANGE type; carry the bounds as two columns by hand"),
}


def worst(severities) -> str:
    return max(severities, key=SEVERITY.index, default="map")


def _decimal(col: dict, default: tuple[int, int]) -> tuple[int, int] | None:
    """(precision, scale) declared on a parameterized NUMERIC/BIGNUMERIC, or the default."""
    p, s = col.get("precision"), col.get("scale")
    if p is None:
        return default
    try:
        return int(p), int(s or 0)
    except (TypeError, ValueError):
        return None


def _scalar(col: dict, *, bignumeric: str, geography: str) -> tuple[str | None, str, str, str]:
    """(Delta type, rule, severity, detail) for one non-repeated value."""
    t = str(col.get("type", "")).upper()
    if t in _SIMPLE:
        target, rule = _SIMPLE[t]
        return target, rule, "map", ""
    if t in ("NUMERIC", "DECIMAL"):
        ps = _decimal(col, (38, 9))
        if ps is None or ps[0] > 38:
            return None, "TY08_NUMERIC", "block", f"unreadable NUMERIC precision {col.get('precision')!r}"
        return f"DECIMAL({ps[0]},{ps[1]})", "TY08_NUMERIC", "map", ""
    if t in ("BIGNUMERIC", "BIGDECIMAL"):
        ps = _decimal(col, (76, 38))
        if ps is not None and ps[0] <= 38:
            # A parameterized BIGNUMERIC(P,S) with P <= 38 fits a Spark DECIMAL exactly.
            return f"DECIMAL({ps[0]},{ps[1]})", "TY09_BIGNUMERIC", "map", ""
        if bignumeric == "string":
            return "STRING", "TY09_BIGNUMERIC", "caveat", (
                "BIGNUMERIC exceeds Spark's 38-digit DECIMAL; carried as exact decimal text, "
                "so arithmetic and ordering need an explicit cast")
        return None, "TY09_BIGNUMERIC", "block", (
            "BIGNUMERIC exceeds Spark's 38-digit DECIMAL; re-plan with --bignumeric string to carry it as text")
    if t == "DATETIME":
        return "TIMESTAMP", "TY10_DATETIME", "caveat", (
            "DATETIME has no time zone; AIDP refuses TIMESTAMP_NTZ in CREATE TABLE, so it is carried as "
            "TIMESTAMP and read back through the session time zone: keep spark.sql.session.timeZone=UTC")
    if t == "TIME":
        return "STRING", "TY11_TIME", "flag", (
            "no Spark TIME type; carried as 'HH:MM:SS[.ffffff]' text, so comparison, ordering and "
            "time arithmetic become string operations")
    if t == "JSON":
        return "STRING", "TY14_JSON", "caveat", (
            "carried as JSON text; read fields with get_json_object/from_json, and note a JSON path "
            "query in a view is flagged by the SQL translator")
    if t == "GEOGRAPHY":
        if geography == "wkt":
            return "STRING", "TY15_GEOGRAPHY", "caveat", (
                "carried as WKT text: no spatial type, index or predicate on the target")
        return None, "TY15_GEOGRAPHY", "block", (
            "no Delta spatial type; re-plan with --geography wkt to carry it as WKT text")
    if t in _BLOCKED:
        rule, detail = _BLOCKED[t]
        return None, rule, "block", detail
    return None, "TY99_UNKNOWN", "block", f"unmapped BigQuery type {t!r} is never approximated"


def map_column(col: dict, *, bignumeric: str = "block", geography: str = "block") -> dict:
    """One planned column: target type, every rule applied, worst severity, details."""
    rules: list[str] = []
    details: list[str] = []
    severities: list[str] = []
    t = str(col.get("type", "")).upper()

    if t in ("RECORD", "STRUCT"):
        fields = [map_column(f, bignumeric=bignumeric, geography=geography) for f in col.get("fields", [])]
        rules.append("TY12_STRUCT")
        severities.append("map")
        if not fields:
            severities.append("block")
            details.append("STRUCT with no fields")
        for f in fields:
            rules += f["rules"]
            severities.append(f["severity"])
            details += [f"{f['name']}: {d}" for d in f["details"]]
        blocked = not fields or any(f["severity"] == "block" for f in fields)
        target = None if blocked else "STRUCT<" + ", ".join(
            f"{quote_ident(f['name'])}: {f['target_type']}" for f in fields) + ">"
    else:
        target, rule, severity, detail = _scalar(col, bignumeric=bignumeric, geography=geography)
        rules.append(rule)
        severities.append(severity)
        if detail:
            details.append(detail)

    if str(col.get("mode", "")).upper() == "REPEATED":
        rules.append("TY13_ARRAY")
        target = f"ARRAY<{target}>" if target else None

    severity = worst(severities)
    out = {
        "name": col["name"],
        "source_type": t + (" REPEATED" if str(col.get("mode", "")).upper() == "REPEATED" else ""),
        "target_type": target if severity != "block" else None,
        "nullable": str(col.get("mode", "")).upper() != "REQUIRED",
        "rules": list(dict.fromkeys(rules)),
        "severity": severity,
        "details": details,
    }
    if col.get("description"):
        out["comment"] = col["description"]
    return out


def map_type_name(name: str) -> tuple[str | None, str, str, str]:
    """A GoogleSQL type name in SQL text (CAST, function arguments) → Spark type name."""
    return _scalar({"type": name}, bignumeric="block", geography="block")


def quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"
