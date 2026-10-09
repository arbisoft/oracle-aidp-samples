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

BQ_NOTEBOOKS = [
    {"name": "Customer segmentation exploration", "id": "nb-0001"},
    {"name": "Revenue anomaly triage", "id": "nb-0002"},
    {"name": "Ad spend data checks", "id": "nb-0003"},
]

BQ_PIPELINES = [{"name": "nightly_marketing_rollup", "id": "pl-0001"}]

GCS_BUCKETS = [
    {"name": "northwind-landing", "location": "US", "storage_class": "STANDARD"},
    {"name": "northwind-logs-archive", "location": "US", "storage_class": "COLDLINE"},
    {"name": "northwind-ml-artifacts", "location": "US-CENTRAL1", "storage_class": "STANDARD"},
    {"name": "northwind-composer-dags", "location": "US-CENTRAL1", "storage_class": "STANDARD"},
]

DATAPROC = {
    "clusters": [
        {"name": "etl-nightly", "region": "us-central1", "image_version": "2.2-debian12",
         "worker_count": 8, "machine_type": "n2-standard-8"},
        {"name": "adhoc-analytics", "region": "us-central1", "image_version": "2.1-debian11",
         "worker_count": 2, "machine_type": "n2-standard-4"},
    ],
    "jobs": [
        {"id": "job-sessionize-01", "cluster": "etl-nightly", "job_type": "pyspark",
         "main_file": "gs://northwind-landing/jobs/sessionize.py"},
        {"id": "job-dedupe-events", "cluster": "etl-nightly", "job_type": "pyspark",
         "main_file": "gs://northwind-landing/jobs/dedupe_events.py"},
        {"id": "job-gl-export", "cluster": "etl-nightly", "job_type": "spark",
         "main_file": "gs://northwind-landing/jobs/gl-export.jar"},
        {"id": "job-cohort-sql", "cluster": "adhoc-analytics", "job_type": "spark_sql",
         "main_file": "gs://northwind-landing/jobs/cohorts.sql"},
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

DATAFORM = {"repositories": [{"name": "northwind-transformations", "region": "us-central1"}]}

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
