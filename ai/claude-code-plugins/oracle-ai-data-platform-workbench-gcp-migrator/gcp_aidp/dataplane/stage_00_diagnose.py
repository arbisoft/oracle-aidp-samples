"""00 diagnose: run first. Reads, writes nothing.

Checks the session, the credential, the target catalog, the connector and, for
tables on the export route, that their Parquet files on OCI can be read; and
prints how the connector types every planned column next to the target type.
Reading each table's column list (no rows) is the connector check.
The type table is the check the plan asks for: confirm each type mapping
against what the connector returns.

What would otherwise fail later is a problem here, before anything is created: a
source whose columns differ from the plan (the copy would stop), and a target table
already there with another layout (structure leaves it as it is).
"""
from gcp_aidp.dataplane.common import *  # inlined above in the notebook


def main(spark, params: dict, plan: dict) -> list[str]:
    problems = []
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print(f"spark {spark.version}, session time zone {spark.conf.get('spark.sql.session.timeZone')}")

    try:
        read = bigquery_reader(spark, gcp_credentials(params), plan["project"])
    except Exception as exc:  # noqa: BLE001
        problems.append(f"credential: {exc}")
        read = None

    try:
        spark.sql(f"SHOW SCHEMAS IN {quote(plan['catalog'])}").collect()
        print(f"target catalog {plan['catalog']}: reachable")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"target catalog {plan['catalog']}: {error_text(exc, 160)}")

    for t in plan["tables"]:  # export route: the files must be on OCI before the copy
        if t.get("files"):
            try:
                names = spark.read.parquet(t["files"]).schema.names  # reading the footers is the check
                print(f"export files {t['dataset']}.{t['name']}: readable at {t['files']}")
                if t.get("rows_file"):
                    print(f"  {exported_rows(spark, t['rows_file']):,} rows exported (the count the copy checks)")
                drift = column_drift([c["name"] for c in t["columns"]], names)[2]
                if drift:
                    problems.append(f"{t['dataset']}.{t['name']}: the export files' columns differ from the plan: "
                                    f"{drift} (plan, migrate and export again)")
            except Exception as exc:  # noqa: BLE001
                problems.append(f"{t['dataset']}.{t['name']}: export files not readable at {t['files']} "
                                f"(run `gcp-aidp export --apply` first): {error_text(exc, 160)}")

    if read is not None:
        print(f"\n{'table.column':<45} {'BigQuery':<18} {'connector (Spark)':<28} target")
        for t in plan["tables"]:
            if t.get("rows_file"):  # the connector refuses the table; its export files were checked above
                print(f"{t['dataset'] + '.' + t['name']:<45} read from its export files, not the connector")
                continue
            try:
                schema = {f.name: f.dataType.simpleString() for f in read(t["source"]).schema.fields}
            except Exception as exc:  # noqa: BLE001
                text = error_text(exc, 160)
                if "bigquery" in text.lower() and ("data source" in text.lower() or "ClassNotFound" in text):
                    # Reading the schema is the connector check: a class lookup through the
                    # JVM's system class loader misses a JAR installed as a cluster library.
                    problems.append(f"connector JAR not on this cluster (install it, then restart): {text}")
                    break
                problems.append(f"{t['source']}: {text}")
                continue
            drift = column_drift([c["name"] for c in t["columns"]], list(schema))[2]
            if drift and not t.get("files"):  # an export-route table is read from its files, checked above
                problems.append(f"{t['source']}: the connector's columns differ from the plan: {drift} "
                                "(the copy would stop; inventory and plan again)")
            for c in t["columns"]:
                print(f"{t['dataset'] + '.' + t['name'] + '.' + c['name']:<45} {c['source_type']:<18} "
                      f"{schema.get(c['name'], 'MISSING'):<28} {c['target_type']}")

    for t in plan["tables"]:  # structure never alters a table that is already there
        try:
            drift = table_exists(spark, quote(*t["target"])) and layout_drift(spark, t)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{'.'.join(t['target'])}: {error_text(exc, 160)}")
            continue
        if drift:
            problems.append(f"{'.'.join(t['target'])} already exists with another layout: {'; '.join(drift)} "
                            "(structure leaves it as it is: drop it if it is empty, then run again)")
    print()
    for p in problems:
        print("PROBLEM:", p)
    print("diagnose:", "OK" if not problems else f"{len(problems)} problem(s)")
    return problems
