"""00 diagnose: run first. Reads, writes nothing.

Checks the session, the credential, the target catalog and the connector, and
prints how the connector types every planned column next to the target type.
Reading each table's column list (no rows) is the connector check.
The type table is the check the plan asks for: confirm each type mapping
against what the connector returns.
"""
from gcp_aidp.dataplane.common import *  # noqa: F403 - inlined above in the notebook


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

    if read is not None:
        print(f"\n{'table.column':<45} {'BigQuery':<18} {'connector (Spark)':<28} target")
        for t in plan["tables"]:
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
            for c in t["columns"]:
                print(f"{t['dataset'] + '.' + t['name'] + '.' + c['name']:<45} {c['source_type']:<18} "
                      f"{schema.get(c['name'], 'MISSING'):<28} {c['target_type']}")
    print()
    for p in problems:
        print("PROBLEM:", p)
    print("diagnose:", "OK" if not problems else f"{len(problems)} problem(s)")
    return problems
