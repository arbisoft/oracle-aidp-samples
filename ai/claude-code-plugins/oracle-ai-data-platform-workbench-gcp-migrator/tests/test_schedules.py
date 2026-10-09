"""Source schedules → AIDP Quartz, carried on the job and always created PAUSED."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gcp_aidp.migrate import migrate
from gcp_aidp.migrate.runner import _carry, _dataproc_schedule
from gcp_aidp.plan import build_plan
from gcp_aidp.publish import plan_publish
from gcp_aidp.translate.schedules import from_bigquery, from_unix_cron


class UnixCron(unittest.TestCase):
    def test_translated(self):
        for cron, quartz in (("0 2 * * *", "0 0 2 * * ?"), ("*/15 * * * *", "0 */15 * * * ?"),
                             ("0 6 1 * *", "0 0 6 1 * ?"), ("30 2 * * 1-5", "0 30 2 ? * MON,TUE,WED,THU,FRI"),
                             ("0 9 * * 5-7", "0 0 9 ? * SUN,FRI,SAT"), ("0 0 * * */2", "0 0 0 ? * SUN,TUE,THU,SAT"),
                             ("0 9 * jan,jul mon", "0 0 9 ? JAN,JUL MON")):
            self.assertEqual(from_unix_cron(cron), (quartz, ""), cron)

    def test_not_translated(self):
        for cron in ("0 9 1 * 1",  # day of month OR day of week: Quartz has no OR
                     "@daily", "0 9 L * *", "0 9 * * FRI-SUN", "0 9 * *"):
            quartz, why = from_unix_cron(cron)
            self.assertIsNone(quartz, cron)
            self.assertTrue(why, cron)


class BigQuerySchedule(unittest.TestCase):
    def test_translated(self):
        for text, quartz in (("every day 02:00", "0 0 2 * * ?"), ("every monday 03:00", "0 0 3 ? * MON"),
                             ("every mon,wednesday 09:30", "0 30 9 ? * MON,WED"),
                             ("1,15 of month 4:00", "0 0 4 1,15 * ?")):
            self.assertEqual(from_bigquery(text), (quartz, ""), text)

    def test_an_interval_is_placed_by_the_next_run(self):
        self.assertEqual(from_bigquery("every 6 hours", "2026-10-10T02:17:00Z")[0], "0 17 2/6 * * ?")
        self.assertEqual(from_bigquery("every 24 hours", "2026-10-10T02:17:00Z")[0], "0 17 2 * * ?")
        self.assertEqual(from_bigquery("every 15 minutes", "2026-10-10T02:07:00Z")[0], "0 7/15 * * * ?")
        self.assertTrue(from_bigquery("every 6 hours", "2026-10-10T02:17:00Z")[1])  # a caveat, not exact
        for text, nxt in (("every 24 hours", None), ("every 5 hours", "2026-10-10T02:17:00Z"),
                          ("first sunday of quarter 00:00", None)):
            self.assertIsNone(from_bigquery(text, nxt)[0], text)


class Findings(unittest.TestCase):
    def test_J03_SCHEDULE(self):
        schedule, f = _carry("schedule 'every day 02:00'", "0 0 2 * * ?", "", "UTC")
        self.assertEqual(schedule, {"quartzCronExpression": "0 0 2 * * ?", "timezoneId": "UTC"})
        self.assertEqual([(x["rule"], x["severity"]) for x in f], [("J03_SCHEDULE", "rewrite")])
        self.assertEqual(_carry("x", "0 17 2/6 * * ?", "placed by the next run", "UTC")[1][0]["severity"], "caveat")

    def test_J01_UNSCHEDULED(self):
        schedule, f = _carry("schedule 'every 5 hours'", None, "drifts", "UTC")
        self.assertIsNone(schedule)
        self.assertEqual([(x["rule"], x["severity"]) for x in f], [("J01_UNSCHEDULED", "flag")])

    def test_two_scheduler_jobs_on_one_definition_carry_neither(self):
        two = {"schedules": [{"name": "a", "schedule": "0 1 * * *"}, {"name": "b", "schedule": "0 2 * * *"}]}
        schedule, f = _dataproc_schedule(two)
        self.assertIsNone(schedule)
        self.assertEqual(f[0]["rule"], "J01_UNSCHEDULED")
        self.assertEqual(_dataproc_schedule({}), (None, []))


class EndToEnd(unittest.TestCase):
    def test_a_scheduled_query_is_published_with_its_schedule_paused(self):
        queries = [{"id": "q1", "name": "Nightly", "query": "SELECT 1", "schedule": "every day 02:00"},
                   {"id": "q2", "name": "Drifting", "query": "SELECT 2", "schedule": "every 5 hours",
                    "next_run_time": "2026-10-10T02:00:00Z"}]
        plan = build_plan({"project_id": "p", "sources": {"bigquery": {"items": {"scheduled_queries": queries}}}})
        with tempfile.TemporaryDirectory() as tmp:
            migrate(plan, out_dir=Path(tmp))
            jobs = {j["name"]: j["definition"] for j in plan_publish(tmp, cluster_key="ck")["jobs"]}
        self.assertEqual(jobs["Nightly"]["schedule"], {"quartzCronExpression": "0 0 2 * * ?", "timezoneId": "UTC",
                                                       "pauseStatus": "PAUSED"})
        self.assertNotIn("schedule", jobs["Drifting"])  # not exact: created unscheduled, flagged J01


if __name__ == "__main__":
    unittest.main()
