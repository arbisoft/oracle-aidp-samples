"""Local stand-in for connectors/jira/examples/jira_issue_load.ipynb.

Runs the SAME calls the notebook makes (credentials, account_timezone,
search_issues, to_dataframe, Delta write, incremental MERGE) against a real
Jira site and a real local Spark+Delta session. Proves the write/merge path,
which nothing has tested until now. Does NOT prove AIDP-specific things: the
credential store, the real cluster's network/catalog setup, or behaviour on
AIDP's exact Spark 3.5 / Python 3.11 / Java 17 combo (this runs on Spark 3.5.1
/ Python 3.12 / Java 8 locally instead — close, not identical).
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "connectors/jira")
sys.path.insert(0, "connectors/_shared")
sys.path.insert(0, "connectors/jira/spike")

import _env
_env.load(".env")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

import jira as j

WAREHOUSE = os.path.join(os.path.dirname(__file__), "out", "warehouse")
TARGET = "jira_issue_local_test"

builder = (
    SparkSession.builder.master("local[1]")
    .appName("jira-local-run")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE)
    .config("spark.sql.session.timeZone", "UTC")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def run(label):
    print("\n=== %s ===" % label)
    site, email, token = j.credentials_from_env()
    session = j.jira_session(email, token)
    tz_name = j.account_timezone(session, site)

    since = None
    if spark.catalog.tableExists(TARGET):
        since = spark.sql("SELECT max(updated) AS m FROM %s" % TARGET).first()["m"]
        print("since (from target MAX(updated)):", since)

    until = datetime.now(timezone.utc)
    issues = j.search_issues(
        session, site, query="project = KAN", tz_name=tz_name,
        since=since, until=until, overlap_seconds=300, page_size=3,
    )
    df = j.to_dataframe(spark, list(issues))
    print("issues read this run:", df.count())

    if not spark.catalog.tableExists(TARGET):
        df.write.format("delta").saveAsTable(TARGET)
        print("created target table")
    else:
        df.createOrReplaceTempView("incoming_issue")
        spark.sql(
            "MERGE INTO %s t USING incoming_issue s ON t.key = s.key "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *" % TARGET
        )
        print("merged into target table")

    total = spark.table(TARGET).count()
    distinct_keys = spark.sql("SELECT count(DISTINCT key) AS c FROM %s" % TARGET).first()["c"]
    print("target row count:", total, "| distinct keys:", distinct_keys)
    spark.table(TARGET).select("key", "summary", "status", "updated", "raw_fields").orderBy("key").show(20, truncate=40)
    return total, distinct_keys


try:
    if spark.catalog.tableExists(TARGET):
        spark.sql("DROP TABLE %s" % TARGET)

    total1, distinct1 = run("First run (full load)")
    assert total1 == distinct1, "duplicate keys after first load!"

    total2, distinct2 = run("Second run (incremental, no new data)")
    assert total2 == total1, "row count changed on a no-op incremental run: %d -> %d" % (total1, total2)
    assert total2 == distinct2, "duplicate keys after incremental MERGE!"

    print("\nALL LOCAL CHECKS PASSED: write, full load, and no-op incremental MERGE all behave correctly.")
finally:
    spark.stop()
