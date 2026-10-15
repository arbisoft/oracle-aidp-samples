"""Build `demo-manifest.json`: an invented Google Cloud estate, Northwind Retail.

Deterministic, so a rebuild only changes the file when this script changes:

    python3 -m gcp_aidp.fixtures.build_demo_manifest

The estate is larger than the `test-estate/` seed and includes services the
seed cannot create for free (Dataproc, Composer, Dataform, Dataflow, Vertex AI),
so `inventory`, `plan` and SKIP reporting are tested without paying for them.

Column types use the names the BigQuery `tables.get` API returns (`INTEGER`,
`FLOAT`, `BOOLEAN`, `RECORD`), not the GoogleSQL names (`INT64`, ...).
"""
from __future__ import annotations

import json
from pathlib import Path

PROJECT = "northwind-analytics-demo"
LOCATION = "US"


def col(name, type_, mode="NULLABLE", fields=None, description=""):
    c = {"name": name, "type": type_, "mode": mode}
    if fields:
        c["fields"] = fields
    if description:
        c["description"] = description
    return c


def table(dataset, name, columns, *, rows, partition=None, clustering=None,
          description="", labels=None, expiration_ms=None):
    t = {
        "dataset": dataset, "name": name, "type": "TABLE",
        "columns": columns, "num_rows": rows, "num_bytes": rows * 180,
        "description": description, "labels": labels or {},
    }
    if partition:
        t["partitioning"] = partition
    if clustering:
        t["clustering"] = clustering
    if expiration_ms:
        t["expiration_ms"] = expiration_ms
    return t


ADDRESS = [col("street", "STRING"), col("city", "STRING"),
           col("postcode", "STRING"), col("country", "STRING")]

DATASETS = [
    {"name": "sales", "location": LOCATION, "description": "Orders, customers and products"},
    {"name": "marketing", "location": LOCATION, "description": "Campaigns and web events"},
    {"name": "finance", "location": LOCATION, "description": "General ledger and payments"},
    {"name": "logs", "location": LOCATION, "description": "Daily-sharded application logs"},
]


def _tables() -> list[dict]:
    t = [
        # Partitioned on a column and clustered: CLUSTER BY (partition + cluster keys).
        table("sales", "orders", [
            col("order_id", "INTEGER", "REQUIRED"), col("customer_id", "INTEGER"),
            col("status", "STRING"), col("order_date", "DATE"),
            col("total", "NUMERIC"), col("created_at", "TIMESTAMP"),
        ], rows=2_400_000, partition={"type": "DAY", "field": "order_date"},
            clustering=["customer_id", "status"], description="One row per order"),
        table("sales", "order_items", [
            col("order_id", "INTEGER", "REQUIRED"), col("line_no", "INTEGER"),
            col("product_id", "INTEGER"), col("quantity", "INTEGER"),
            col("unit_price", "NUMERIC"), col("discount", "FLOAT"),
        ], rows=7_100_000, clustering=["order_id"]),
        # STRUCT and ARRAY columns.
        table("sales", "customers", [
            col("customer_id", "INTEGER", "REQUIRED"), col("name", "STRING"),
            col("email", "STRING"), col("is_active", "BOOLEAN"),
            col("address", "RECORD", fields=ADDRESS),
            col("tags", "STRING", "REPEATED"),
            col("orders_summary", "RECORD", "REPEATED", fields=[
                col("year", "INTEGER"), col("order_count", "INTEGER"),
            ]),
        ], rows=310_000, description="Customer master"),
        table("sales", "products", [
            col("product_id", "INTEGER", "REQUIRED"), col("sku", "STRING"),
            col("name", "STRING"), col("category", "STRING"),
            col("list_price", "NUMERIC"), col("thumbnail", "BYTES"),
        ], rows=18_000),
        # Ingestion-time partitioning: no column to carry, flagged.
        table("sales", "returns", [
            col("order_id", "INTEGER"), col("reason", "STRING"), col("refund", "NUMERIC"),
        ], rows=95_000, partition={"type": "DAY", "field": None}),
        # Every type the mapper must decide: JSON, GEOGRAPHY, BIGNUMERIC, DATETIME, TIME,
        # INTERVAL, RANGE.
        table("sales", "store_visits", [
            col("visit_id", "STRING", "REQUIRED"), col("store_location", "GEOGRAPHY"),
            col("payload", "JSON"), col("basket_value", "BIGNUMERIC"),
            col("visited_at_local", "DATETIME"), col("opening_time", "TIME"),
            col("dwell", "INTERVAL"), col("promo_window", "RANGE"),
        ], rows=1_200_000, partition={"type": "MONTH", "field": "visited_at_local"}),
        table("marketing", "campaigns", [
            col("campaign_id", "INTEGER", "REQUIRED"), col("name", "STRING"),
            col("channel", "STRING"), col("budget", "NUMERIC"),
            col("start_date", "DATE"), col("end_date", "DATE"),
        ], rows=1_400, labels={"owner": "growth"}),
        table("marketing", "web_events", [
            col("event_id", "STRING", "REQUIRED"), col("event_ts", "TIMESTAMP"),
            col("user_pseudo_id", "STRING"), col("event_name", "STRING"),
            col("params", "RECORD", "REPEATED", fields=[
                col("key", "STRING"),
                col("value", "RECORD", fields=[col("string_value", "STRING"),
                                               col("int_value", "INTEGER")]),
            ]),
        ], rows=88_000_000, partition={"type": "DAY", "field": "event_ts"},
            clustering=["event_name"], expiration_ms=7_776_000_000),
        # Integer-range partitioning.
        table("marketing", "audience_segments", [
            col("segment_id", "INTEGER", "REQUIRED"), col("name", "STRING"),
            col("member_count", "INTEGER"),
        ], rows=640, partition={"type": "RANGE", "field": "segment_id",
                                "range": {"start": 0, "end": 1000, "interval": 100}}),
        table("finance", "gl_entries", [
            col("entry_id", "INTEGER", "REQUIRED"), col("account", "STRING"),
            col("amount", "NUMERIC"), col("currency", "STRING"),
            col("posted_on", "DATE"), col("posted_by", "STRING"),
        ], rows=5_600_000, partition={"type": "MONTH", "field": "posted_on"},
            clustering=["account"]),
        table("finance", "payments", [
            col("payment_id", "STRING", "REQUIRED"), col("order_id", "INTEGER"),
            col("amount", "NUMERIC"), col("method", "STRING"),
            col("paid_at", "TIMESTAMP"), col("card_last4", "STRING"),
        ], rows=2_350_000),
        table("finance", "fx_rates", [
            col("rate_date", "DATE"), col("from_ccy", "STRING"),
            col("to_ccy", "STRING"), col("rate", "BIGNUMERIC"),
        ], rows=52_000),
    ]
    # A plain reporting layer: enough tables that the plan reads like a real estate.
    for dataset, names in {
        "sales": ["daily_revenue", "weekly_revenue", "monthly_revenue",
                  "category_revenue", "store_revenue", "customer_ltv",
                  "repeat_purchase", "basket_size"],
        "marketing": ["campaign_daily", "channel_mix", "attribution_last_touch",
                      "attribution_first_touch", "email_sends", "email_opens"],
        "finance": ["trial_balance", "ar_aging", "ap_aging", "cash_position",
                    "budget_vs_actual", "cost_centres"],
    }.items():
        for i, name in enumerate(names):
            t.append(table(dataset, name, [
                col("report_date", "DATE", "REQUIRED"), col("dimension", "STRING"),
                col("metric_value", "NUMERIC"), col("row_count", "INTEGER"),
                col("refreshed_at", "TIMESTAMP"),
            ], rows=10_000 * (i + 1)))
    # Date-sharded tables (`events_YYYYMMDD`), the BigQuery pre-partitioning idiom.
    for day in ("20260901", "20260902", "20260903"):
        t.append(table("logs", f"app_events_{day}", [
            col("ts", "TIMESTAMP"), col("level", "STRING"),
            col("message", "STRING"), col("attributes", "JSON"),
        ], rows=4_000_000))
    return t


VIEWS = [
    {"dataset": "sales", "name": "v_active_customers",
     "query": "SELECT customer_id, name, email\n"
              "FROM `northwind-analytics-demo.sales.customers`\nWHERE is_active"},
    {"dataset": "sales", "name": "v_order_kpis",
     "query": "SELECT order_date,\n"
              "       COUNTIF(status = 'RETURNED') AS returned_orders,\n"
              "       SAFE_DIVIDE(SUM(total), COUNT(*)) AS avg_order_value,\n"
              "       TIMESTAMP_TRUNC(MAX(created_at), DAY) AS last_order_day\n"
              "FROM `northwind-analytics-demo.sales.orders`\nGROUP BY order_date"},
    {"dataset": "sales", "name": "v_latest_order_per_customer",
     "query": "SELECT * FROM `northwind-analytics-demo.sales.orders`\n"
              "QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) = 1"},
    {"dataset": "sales", "name": "v_customer_tags",
     "query": "SELECT c.customer_id, tag\n"
              "FROM `northwind-analytics-demo.sales.customers` AS c, UNNEST(c.tags) AS tag"},
    {"dataset": "marketing", "name": "v_campaign_days",
     "query": "SELECT campaign_id, DATE_DIFF(end_date, start_date, DAY) AS days\n"
              "FROM `northwind-analytics-demo.marketing.campaigns`"},
    {"dataset": "marketing", "name": "v_events_no_params",
     "query": "SELECT * EXCEPT (params)\nFROM `northwind-analytics-demo.marketing.web_events`"},
    {"dataset": "finance", "name": "v_payments_masked",
     "query": "SELECT payment_id, order_id, amount, method,\n"
              "       FORMAT_DATE('%Y-%m-%d', DATE(paid_at)) AS paid_on\n"
              "FROM `northwind-analytics-demo.finance.payments`"},
    {"dataset": "logs", "name": "v_all_app_events",
     "query": "SELECT _TABLE_SUFFIX AS day, *\n"
              "FROM `northwind-analytics-demo.logs.app_events_*`"},
]

MATERIALIZED_VIEWS = [
    {"dataset": "sales", "name": "mv_daily_sales",
     "query": "SELECT order_date, COUNT(*) AS orders, SUM(total) AS revenue\n"
              "FROM `northwind-analytics-demo.sales.orders`\nGROUP BY order_date",
     "refresh_interval_minutes": 60},
    {"dataset": "marketing", "name": "mv_events_by_name",
     "query": "SELECT event_name, DATE(event_ts) AS day, COUNT(*) AS events\n"
              "FROM `northwind-analytics-demo.marketing.web_events`\nGROUP BY 1, 2",
     "refresh_interval_minutes": 30},
]

EXTERNAL_TABLES = [
    {"dataset": "marketing", "name": "ext_ad_spend", "format": "PARQUET",
     "source_uris": ["gs://northwind-landing/ad_spend/*.parquet"]},
    {"dataset": "finance", "name": "ext_bank_statements", "format": "CSV",
     "source_uris": ["gs://northwind-landing/bank/*.csv"]},
    {"dataset": "logs", "name": "ext_cdn_logs", "format": "NEWLINE_DELIMITED_JSON",
     "source_uris": ["gs://northwind-logs-archive/cdn/2026/*.json"]},
]

ROUTINES = [
    {"dataset": "sales", "name": "net_price", "routine_type": "SCALAR_FUNCTION",
     "language": "SQL", "body": "price * (1 - IFNULL(discount, 0))",
     "arguments": [{"name": "price", "type": "NUMERIC"}, {"name": "discount", "type": "FLOAT64"}],
     "return_type": "NUMERIC"},
    {"dataset": "sales", "name": "safe_ratio", "routine_type": "SCALAR_FUNCTION",
     "language": "SQL", "body": "SAFE_DIVIDE(a, b)",
     "arguments": [{"name": "a", "type": "FLOAT64"}, {"name": "b", "type": "FLOAT64"}],
     "return_type": "FLOAT64"},
    {"dataset": "marketing", "name": "parse_utm", "routine_type": "SCALAR_FUNCTION",
     "language": "JAVASCRIPT",
     "body": "var m = /utm_source=([^&]+)/.exec(url);\nreturn m ? m[1] : null;",
     "arguments": [{"name": "url", "type": "STRING"}], "return_type": "STRING"},
    {"dataset": "sales", "name": "orders_for_customer", "routine_type": "TABLE_VALUED_FUNCTION",
     "language": "SQL",
     "body": "SELECT * FROM `northwind-analytics-demo.sales.orders` WHERE customer_id = cid",
     "arguments": [{"name": "cid", "type": "INT64"}]},
    {"dataset": "finance", "name": "close_month", "routine_type": "PROCEDURE",
     "language": "SQL",
     "body": "BEGIN\n  DECLARE period DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);\n"
             "  DELETE FROM `northwind-analytics-demo.finance.trial_balance` WHERE report_date = period;\n"
             "  INSERT INTO `northwind-analytics-demo.finance.trial_balance`\n"
             "  SELECT period, account, SUM(amount), COUNT(*), CURRENT_TIMESTAMP()\n"
             "  FROM `northwind-analytics-demo.finance.gl_entries`\n"
             "  WHERE DATE_TRUNC(posted_on, MONTH) = period GROUP BY account;\nEND",
     "arguments": []},
]

MODELS = [
    {"dataset": "marketing", "name": "churn_model", "model_type": "LOGISTIC_REG"},
    {"dataset": "sales", "name": "demand_forecast", "model_type": "ARIMA_PLUS"},
]

SAVED_QUERIES = [
    {"name": "Top products last 30 days",
     "query": "SELECT p.name, SUM(i.quantity) AS units\n"
              "FROM `northwind-analytics-demo.sales.order_items` i\n"
              "JOIN `northwind-analytics-demo.sales.products` p USING (product_id)\n"
              "GROUP BY p.name ORDER BY units DESC LIMIT 20"},
    {"name": "Refund rate by reason",
     "query": "SELECT reason, COUNT(*) AS n\nFROM `northwind-analytics-demo.sales.returns`\nGROUP BY reason"},
    {"name": "Campaign ROI",
     "query": "SELECT c.name, SAFE_DIVIDE(SUM(o.total), c.budget) AS roi\n"
              "FROM `northwind-analytics-demo.marketing.campaigns` c\n"
              "JOIN `northwind-analytics-demo.sales.orders` o\n"
              "  ON o.order_date BETWEEN c.start_date AND c.end_date\nGROUP BY c.name, c.budget"},
    {"name": "Churn scoring",
     "query": "SELECT * FROM ML.PREDICT(MODEL `northwind-analytics-demo.marketing.churn_model`,\n"
              "  TABLE `northwind-analytics-demo.sales.customers`)"},
]

SCHEDULED_QUERIES = [
    {"id": "6512f0a1-0000-2b8e-a1d4-001a11440001", "name": "Refresh daily_revenue",
     "schedule": "every day 02:00", "destination_dataset": "sales",
     "query": "SELECT order_date AS report_date, 'all' AS dimension, SUM(total) AS metric_value,\n"
              "       COUNT(*) AS row_count, CURRENT_TIMESTAMP() AS refreshed_at\n"
              "FROM `northwind-analytics-demo.sales.orders`\n"
              "WHERE order_date = DATE_SUB(@run_date, INTERVAL 1 DAY)\nGROUP BY order_date"},
    {"id": "6512f0a1-0000-2b8e-a1d4-001a11440002", "name": "Refresh channel_mix",
     "schedule": "every monday 03:00", "destination_dataset": "marketing",
     "query": "SELECT CURRENT_DATE() AS report_date, channel AS dimension, SUM(budget) AS metric_value,\n"
              "       COUNT(*) AS row_count, CURRENT_TIMESTAMP() AS refreshed_at\n"
              "FROM `northwind-analytics-demo.marketing.campaigns`\nGROUP BY channel"},
    {"id": "6512f0a1-0000-2b8e-a1d4-001a11440003", "name": "Month-end close",
     "schedule": "1 of month 04:00", "destination_dataset": None,
     "query": "CALL `northwind-analytics-demo.finance.close_month`()"},
]

ACCESS_POLICIES = [
    {"kind": "row_access_policy", "dataset": "finance", "table": "payments",
     "name": "emea_only", "filter": "region = 'EMEA'", "grantees": ["group:finance-emea@example.com"]},
    {"kind": "column_policy_tag", "dataset": "finance", "table": "payments",
     "column": "card_last4", "policy_tag": "projects/northwind-analytics-demo/locations/us/taxonomies/1/policyTags/pci"},
    {"kind": "column_policy_tag", "dataset": "sales", "table": "customers",
     "column": "email", "policy_tag": "projects/northwind-analytics-demo/locations/us/taxonomies/1/policyTags/pii"},
    {"kind": "dataset_iam", "dataset": "finance",
     "role": "roles/bigquery.dataViewer", "members": ["group:finance-analysts@example.com"]},
]

# Two notebooks as exported .ipynb (cell sources only); the third is listed, not exported.
BQ_NOTEBOOKS = [
    {"name": "Customer segmentation exploration", "id": "nb-0001", "cells": [
        {"cell_type": "markdown", "source": "# Customer segmentation\nOrders per customer, by country."},
        {"cell_type": "code", "source": "%load_ext bigquery_magics\n!pip install seaborn\nimport seaborn as sns\n"
                                        "import bigframes.pandas as bpd"},
        {"cell_type": "code", "source": "%%bigquery segments\nSELECT c.country, COUNT(*) AS orders,\n"
                                        "       COUNTIF(o.status = 'Returned') AS returned\n"
                                        "FROM `northwind-analytics-demo.sales.orders` o\n"
                                        "JOIN `northwind-analytics-demo.sales.customers` c USING (customer_id)\n"
                                        "GROUP BY c.country"},
        {"cell_type": "code", "source": "%%bigquery products\nnorthwind-analytics-demo.sales.products"},
        {"cell_type": "code", "source": "sns.barplot(data=segments, x=\"country\", y=\"orders\")"},
    ]},
    {"name": "Revenue anomaly triage", "id": "nb-0002", "cells": [
        {"cell_type": "code", "source": "%%bigquery daily\nSELECT order_date, SAFE_DIVIDE(SUM(amount), COUNT(*)) AS avg_order\n"
                                        "FROM `northwind-analytics-demo.sales.orders`\nGROUP BY order_date"},
        {"cell_type": "code", "source": "import pandas as pd\n"
                                        "targets = pd.read_csv(\"gs://northwind-landing/finance/targets.csv\")"},
        {"cell_type": "code", "source": "%%bigquery legacy --use_legacy_sql\n"
                                        "SELECT order_id FROM [northwind-analytics-demo:sales.orders] LIMIT 10"},
    ]},
    {"name": "Ad spend data checks", "id": "nb-0003"},
]

BQ_PIPELINES = [{"name": "nightly_marketing_rollup", "id": "pl-0001"}]

GCS_BUCKETS = [
    {"name": "northwind-landing", "location": "US", "storage_class": "STANDARD"},
    {"name": "northwind-logs-archive", "location": "US", "storage_class": "COLDLINE"},
    {"name": "northwind-ml-artifacts", "location": "US-CENTRAL1", "storage_class": "STANDARD"},
    {"name": "northwind-composer-dags", "location": "US-CENTRAL1", "storage_class": "STANDARD"},
]

# Dataproc job definitions, as the inventory groups the run history: the newest run's
# settings, a run count and the code it reads from Cloud Storage. The scripts read
# BigQuery tables through the connector and gs:// paths, which is what 0.2 translates.
SESSIONIZE_PY = """from pyspark.sql import SparkSession, functions as F
import sys

run_date = sys.argv[1]
spark = SparkSession.builder.appName("sessionize").getOrCreate()

visits = (spark.read.format("bigquery")
          .option("table", "northwind-analytics-demo.sales.store_visits")
          .load()
          .where(F.col("visit_date") == run_date))
events = spark.read.json(f"gs://northwind-landing/clickstream/dt={run_date}/")

sessions = (events.groupBy("customer_id", F.window("event_ts", "30 minutes"))
            .agg(F.count("*").alias("events")))
sessions.write.mode("overwrite").parquet(f"gs://northwind-landing/sessions/dt={run_date}/")
"""

DEDUPE_PY = """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("temporaryGcsBucket", "northwind-landing")

raw = spark.read.format("bigquery").load("northwind-analytics-demo.logs.app_events")
clean = raw.dropDuplicates(["event_id"])
clean.write.format("bigquery").option("table", "logs.app_events_clean").mode("overwrite").save()
spark.sql("SELECT COUNT(*) FROM logs.app_events_clean").show()
"""

COHORTS_SQL = """CREATE OR REPLACE TEMPORARY VIEW first_orders AS
SELECT customer_id, MIN(order_date) AS first_order
FROM sales.orders
GROUP BY customer_id;

INSERT OVERWRITE DIRECTORY 'gs://northwind-landing/reports/cohorts/'
USING parquet
SELECT date_trunc('MONTH', first_order) AS cohort, COUNT(*) AS customers
FROM first_orders
GROUP BY 1;
"""

SCORE_RETURNS_PY = """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
returns = spark.read.format("bigquery").load("sales.returns")
returns.groupBy("reason").count().write.mode("overwrite").csv("gs://northwind-landing/reports/returns/")
"""

DATAPROC = {
    "clusters": [
        {"name": "etl-nightly", "region": "us-central1", "state": "RUNNING", "image_version": "2.2-debian12",
         "master_machine_type": "n2-standard-4", "machine_type": "n2-standard-8", "worker_count": 8,
         "secondary_worker_count": 4,
         "autoscaling_policy": "projects/northwind-analytics-demo/regions/us-central1/autoscalingPolicies/etl",
         "properties": {"spark:spark.executor.memory": "12g", "spark:spark.sql.shuffle.partitions": "400"},
         "init_actions": ["gs://northwind-landing/init/install-deps.sh"]},
        {"name": "adhoc-analytics", "region": "us-central1", "state": "RUNNING", "image_version": "2.1-debian11",
         "master_machine_type": "n2-standard-4", "machine_type": "n2-standard-4", "worker_count": 2,
         "optional_components": ["JUPYTER"]},
    ],
    "jobs": [
        {"id": "us-central1/etl-nightly/sessionize.py", "region": "us-central1", "cluster": "etl-nightly",
         "job_type": "pyspark", "main_file": "gs://northwind-landing/jobs/sessionize.py", "args": ["2026-10-07"],
         "jars": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.45.0.jar"],
         "runs": 412, "last_run": "2026-10-08T02:00:41Z", "last_state": "DONE", "last_run_id": "sessionize-1001",
         "args_vary": True, "code": SESSIONIZE_PY},
        {"id": "us-central1/etl-nightly/dedupe_events.py", "region": "us-central1", "cluster": "etl-nightly",
         "job_type": "pyspark", "main_file": "gs://northwind-landing/jobs/dedupe_events.py",
         "properties": {"spark.executor.cores": "4", "spark.sql.shuffle.partitions": "64"},
         "runs": 412, "last_run": "2026-10-08T02:41:10Z", "last_state": "DONE", "last_run_id": "dedupe-1001",
         "code": DEDUPE_PY},
        {"id": "us-central1/etl-nightly/gl-export.jar", "region": "us-central1", "cluster": "etl-nightly",
         "job_type": "spark", "main_file": "gs://northwind-landing/jobs/gl-export.jar",
         "args": ["--month", "2026-09"], "runs": 13, "last_run": "2026-10-01T05:00:12Z", "last_state": "DONE",
         "last_run_id": "gl-export-13", "args_vary": True},
        {"id": "us-central1/adhoc-analytics/cohorts.sql", "region": "us-central1", "cluster": "adhoc-analytics",
         "job_type": "spark_sql", "main_file": "gs://northwind-landing/jobs/cohorts.sql",
         "runs": 30, "last_run": "2026-10-06T09:15:00Z", "last_state": "DONE", "last_run_id": "cohorts-30",
         "code": COHORTS_SQL},
        {"id": "us-central1/adhoc-analytics/legacy_rollup.hql", "region": "us-central1",
         "cluster": "adhoc-analytics", "job_type": "hive", "main_file": "gs://northwind-landing/jobs/legacy_rollup.hql",
         "runs": 4, "last_run": "2026-09-14T11:00:00Z", "last_state": "ERROR", "last_run_id": "rollup-4"},
    ],
    "batches": [
        {"id": "us-central1/serverless/score_returns.py", "region": "us-central1", "job_type": "pyspark",
         "main_file": "gs://northwind-landing/jobs/score_returns.py", "runtime_version": "2.2",
         "runs": 9, "last_run": "2026-10-05T07:30:00Z", "last_state": "SUCCEEDED", "last_run_id": "score-9",
         "code": SCORE_RETURNS_PY},
    ],
    "workflow_templates": [
        {"id": "month-end-close", "region": "us-central1", "cluster": "month-end-ephemeral",
         "managed_cluster": {"name": "month-end-ephemeral", "region": "us-central1", "image_version": "2.2-debian12",
                             "machine_type": "n2-highmem-8", "worker_count": 4},
         "parameters": ["MONTH"],
         "steps": [
             {"step_id": "extract", "prerequisites": [], "job_type": "pyspark",
              "main_file": "gs://northwind-landing/jobs/dedupe_events.py", "code": DEDUPE_PY},
             {"step_id": "ledger", "prerequisites": ["extract"], "job_type": "spark",
              "main_class": "com.northwind.finance.LedgerClose",
              "jars": ["gs://northwind-landing/jobs/finance-close.jar"], "args": ["--month", "${MONTH}"]},
             {"step_id": "report", "prerequisites": ["ledger"], "job_type": "spark_sql",
              "main_file": "gs://northwind-landing/jobs/cohorts.sql", "code": COHORTS_SQL},
         ]},
    ],
    # Cloud Scheduler jobs that call the Dataproc API, with what each starts.
    "schedules": [
        {"name": "sessionize-nightly", "region": "us-central1", "schedule": "0 2 * * *",
         "time_zone": "America/Chicago", "state": "ENABLED",
         "target": {"kind": "job", "region": "us-central1", "job_type": "pyspark",
                    "main_file": "gs://northwind-landing/jobs/sessionize.py", "cluster": "etl-nightly"}},
        {"name": "score-returns-weekdays", "region": "us-central1", "schedule": "30 7 * * 1-5",
         "time_zone": "UTC", "state": "ENABLED",
         "target": {"kind": "batch", "region": "us-central1", "job_type": "pyspark",
                    "main_file": "gs://northwind-landing/jobs/score_returns.py"}},
        {"name": "month-end-close-trigger", "region": "us-central1", "schedule": "0 6 1 * *",
         "time_zone": "America/New_York", "state": "ENABLED",
         "target": {"kind": "workflow_template", "region": "us-central1", "id": "month-end-close"}},
        {"name": "legacy-rollup-weekly", "region": "us-central1", "schedule": "0 4 * * 0",
         "time_zone": "UTC", "state": "PAUSED",
         "target": {"kind": "workflow_template", "region": "us-central1", "id": "legacy-rollup"}},
    ],
}

COMPOSER = {
    "environments": [{"name": "northwind-orchestration", "region": "us-central1",
                      "image_version": "composer-2.9.7-airflow-2.9.3"}],
    "dags": [
        {"environment": "northwind-orchestration", "dag_id": dag}
        for dag in ("daily_sales_load", "marketing_attribution", "finance_month_end",
                    "logs_compaction", "ml_feature_refresh", "data_quality_checks")
    ],
}

def _t(schema, name):
    return {"database": PROJECT, "schema": schema, "name": name}


ORDER_TOTALS = "`northwind-analytics-demo.sales.order_totals_by_day`"
# Compiled actions as `compilationResults.query` returns them, reduced to the fields the migrator uses.
DATAFORM = {"repositories": [
    {"name": "northwind-transformations", "region": "us-central1",
     "compiled_from": "an enabled release config (production-2026-10-01)",
     "schedules": [
         {"kind": "release_config", "name": "production", "cronSchedule": "0 4 * * *",
          "timeZone": "America/New_York", "disabled": "false"},
         {"kind": "workflow_config", "name": "nightly", "cronSchedule": "30 4 * * *",
          "timeZone": "America/New_York", "disabled": "false"}],
     "actions": [
         {"target": _t("sales", "orders"), "filePath": "definitions/sources.js", "declaration": {}},
         {"target": _t("sales", "order_totals_by_day"), "filePath": "definitions/order_totals_by_day.sqlx",
          "relation": {"relationType": "TABLE",
                       "selectQuery": "SELECT order_date, COUNT(*) AS orders, SUM(total) AS revenue\n"
                                      "FROM `northwind-analytics-demo.sales.orders`\nGROUP BY order_date",
                       "dependencyTargets": [_t("sales", "orders")], "tags": ["daily"]}},
         {"target": _t("sales", "v_order_totals_nonempty"), "filePath": "definitions/v_order_totals_nonempty.sqlx",
          "relation": {"relationType": "VIEW",
                       "selectQuery": f"SELECT order_date, orders, revenue\nFROM {ORDER_TOTALS}\n"
                                      "WHERE orders > 0",
                       "dependencyTargets": [_t("sales", "order_totals_by_day")], "tags": ["daily"]}},
         {"target": _t("dataform_assertions", "order_totals_by_day_assertions_uniqueKey_0"),
          "filePath": "definitions/order_totals_by_day.sqlx",
          "assertion": {"selectQuery": "SELECT * FROM (SELECT order_date, COUNT(1) AS index_row_count\n"
                                       f"FROM {ORDER_TOTALS}\nGROUP BY order_date) AS data\n"
                                       "WHERE index_row_count > 1",
                        "dependencyTargets": [_t("sales", "order_totals_by_day")]}},
         {"target": _t("sales", "legacy_order_export"), "filePath": "definitions/legacy_order_export.sqlx",
          "relation": {"relationType": "TABLE", "disabled": True,
                       "selectQuery": "SELECT * FROM `northwind-analytics-demo.sales.orders`",
                       "dependencyTargets": [_t("sales", "orders")]}}]},
    # An incremental table: first-run and merge semantics are not translated, so no job is created.
    {"name": "northwind-events-incremental", "region": "us-central1",
     "compiled_from": "the newest compilation result (8d1f0c2e)", "schedules": [],
     "actions": [
         {"target": _t("marketing", "web_events"), "filePath": "definitions/sources.js", "declaration": {}},
         {"target": _t("marketing", "events_daily"), "filePath": "definitions/events_daily.sqlx",
          "relation": {"relationType": "INCREMENTAL_TABLE",
                       "selectQuery": "SELECT DATE(event_ts) AS event_date, event_name, COUNT(*) AS events\n"
                                      "FROM `northwind-analytics-demo.marketing.web_events`\nGROUP BY 1, 2",
                       "incrementalTableConfig": {
                           "incrementalSelectQuery": "SELECT DATE(event_ts) AS event_date, event_name, COUNT(*) "
                                                     "AS events\nFROM `northwind-analytics-demo.marketing."
                                                     "web_events`\nWHERE event_ts > (SELECT MAX(event_date) "
                                                     "FROM `northwind-analytics-demo.marketing.events_daily`)"
                                                     "\nGROUP BY 1, 2",
                           "uniqueKeyParts": ["event_date", "event_name"]},
                       "dependencyTargets": [_t("marketing", "web_events")]}}]},
]}

DATAFLOW = {"jobs": [
    {"id": "2026-09-01_02_00_00-111", "name": "pubsub-orders-to-bq", "job_type": "JOB_TYPE_STREAMING"},
    {"id": "2026-09-01_03_00_00-222", "name": "gcs-cdn-logs-to-bq", "job_type": "JOB_TYPE_BATCH"},
    {"id": "2026-09-01_04_00_00-333", "name": "crm-sync", "job_type": "JOB_TYPE_BATCH"},
]}

VERTEX = {
    "models": [{"name": "propensity-xgb", "id": "vm-001"}, {"name": "reco-two-tower", "id": "vm-002"}],
    "endpoints": [{"name": "reco-online", "id": "ve-001"}],
    "pipelines": [{"name": "propensity-training", "id": "vp-001"}],
}


def build() -> dict:
    tables = _tables()
    bq_items = {
        "datasets": DATASETS, "tables": tables, "views": VIEWS,
        "materialized_views": MATERIALIZED_VIEWS, "external_tables": EXTERNAL_TABLES,
        "routines": ROUTINES, "models": MODELS, "saved_queries": SAVED_QUERIES,
        "scheduled_queries": SCHEDULED_QUERIES, "access_policies": ACCESS_POLICIES,
        "notebooks": BQ_NOTEBOOKS, "pipelines": BQ_PIPELINES,
    }
    return {
        "project_id": PROJECT,
        "scanned_at": "2026-10-01T09:00:00Z",
        "customer_persona": "Northwind Retail — sample/fixture, no real data",
        "sources_scanned": ["bigquery", "gcs", "dataproc", "composer",
                            "dataform", "dataflow", "vertex"],
        "sources": {
            "bigquery": {"summary": {k: len(v) for k, v in bq_items.items()}, "items": bq_items},
            "gcs": {"summary": {"buckets": len(GCS_BUCKETS)}, "items": {"buckets": GCS_BUCKETS}},
            "dataproc": {"summary": {k: len(v) for k, v in DATAPROC.items()}, "items": DATAPROC},
            "composer": {"summary": {k: len(v) for k, v in COMPOSER.items()}, "items": COMPOSER},
            "dataform": {"summary": {k: len(v) for k, v in DATAFORM.items()}, "items": DATAFORM},
            "dataflow": {"summary": {k: len(v) for k, v in DATAFLOW.items()}, "items": DATAFLOW},
            "vertex": {"summary": {k: len(v) for k, v in VERTEX.items()}, "items": VERTEX},
        },
    }


if __name__ == "__main__":
    out = Path(__file__).with_name("demo-manifest.json")
    out.write_text(json.dumps(build(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {out}")
