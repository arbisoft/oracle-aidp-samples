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
# A type with no Delta column type is blocked by default; `plan --<mode> <choice>` carries it.
# rule: (mode, choice, what carrying it gives up), for the plan's "Needs a decision" list.
OPTIONS = {
    "TY09_BIGNUMERIC": ("bignumeric", "string", "carried as exact decimal text; arithmetic and ordering need a cast"),
    "TY15_GEOGRAPHY": ("geography", "wkt", "carried as WKT text; spatial functions need a rewrite"),
    "TY16_INTERVAL": ("interval", "string",
                      "carried as BigQuery's interval text; date arithmetic with it needs a rewrite"),
    "TY17_RANGE": ("range", "struct",
                   "carried as STRUCT<start, end>; range functions become comparisons on the bounds"),
}
MODES = {mode: ("block", choice) for mode, choice, _ in OPTIONS.values()}


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


def _scalar(col: dict, modes: dict) -> tuple[str | None, str, str, str]:
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
        if modes.get("bignumeric") == "string":
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
        if modes.get("geography") == "wkt":
            return "STRING", "TY15_GEOGRAPHY", "caveat", (
                "carried as WKT text: no spatial type, index or predicate on the target")
        return None, "TY15_GEOGRAPHY", "block", (
            "no Delta spatial type; re-plan with --geography wkt to carry it as WKT text")
    if t == "INTERVAL":
        if modes.get("interval") == "string":
            return "STRING", "TY16_INTERVAL", "caveat", (
                "carried as BigQuery's interval text ('Y-M D H:M:S'); date arithmetic with it needs a rewrite")
        return None, "TY16_INTERVAL", "block", (
            "no Delta INTERVAL column type; re-plan with --interval string to carry it as text")
    if t == "RANGE":
        element = (col.get("rangeElementType") or {}).get("type")
        bound = _scalar({"type": element}, modes) if element else (None, "", "block", "")
        if bound[0] is None:
            return None, "TY17_RANGE", "block", f"RANGE of unreadable element type {element!r}"
        if modes.get("range") == "struct":
            return f"STRUCT<`start`: {bound[0]}, `end`: {bound[0]}>", "TY17_RANGE", "caveat", (
                f"carried as STRUCT<start, end> of {element}, NULL for an unbounded end; "
                "range functions become comparisons on the bounds" + (f"; {bound[3]}" if bound[3] else ""))
        return None, "TY17_RANGE", "block", (
            "no Delta RANGE type; re-plan with --range struct to carry the bounds as STRUCT<start, end>")
    return None, "TY99_UNKNOWN", "block", f"unmapped BigQuery type {t!r} is never approximated"


def map_column(col: dict, **modes: str) -> dict:
    """One planned column: target type, every rule applied, worst severity, details."""
    rules: list[str] = []
    details: list[str] = []
    severities: list[str] = []
    t = str(col.get("type", "")).upper()

    if t in ("RECORD", "STRUCT"):
        fields = [map_column(f, **modes) for f in col.get("fields", [])]
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
        target, rule, severity, detail = _scalar(col, modes)
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


def connector_unreadable(col: dict) -> list[str]:
    """The types in this column the Spark BigQuery connector cannot read. Seen on AIDP with
    connector 0.45: it refuses the whole table, while reading its schema, for an INTERVAL
    ("Unexpected type: INTERVAL") and for a BIGNUMERIC wider than 38 digits. RANGE is
    routed the same way: not yet seen through the connector, and EXPORT DATA converts it
    in BigQuery."""
    t = str(col.get("type", "")).upper()
    if t in ("RECORD", "STRUCT"):
        return [x for f in col.get("fields", []) for x in connector_unreadable(f)]
    if t in ("INTERVAL", "RANGE"):
        return [t]
    if t in ("BIGNUMERIC", "BIGDECIMAL"):
        ps = _decimal(col, (76, 38))
        return [] if ps is not None and ps[0] <= 38 else [t]
    return []


def map_type_name(name: str) -> tuple[str | None, str, str, str]:
    """A GoogleSQL type name in SQL text (CAST, function arguments) → Spark type name."""
    return _scalar({"type": name}, {})


def quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"
