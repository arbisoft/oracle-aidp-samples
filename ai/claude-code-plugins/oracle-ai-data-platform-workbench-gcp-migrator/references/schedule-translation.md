# Schedules → AIDP

Source: `gcp_aidp/translate/schedules.py`. Tests: `tests/test_schedules.py`.

An AIDP job takes `schedule{quartzCronExpression, timezoneId, pauseStatus}`. A migrated
job carries its source schedule when it translates exactly, and is **always created
PAUSED**. Nothing runs until someone turns the schedule on in AIDP, after checking the job.

Measured on AIDP, 2026-10-09: `workflow create-job` accepted a PAUSED schedule and
`get-job` returned it as sent (`0 30 2 ? * MON-FRI` in `America/Los_Angeles` included).
A paused job set to fire every minute started no run in four minutes. We have not yet
checked that a schedule fires on time once it is turned on.

## Where schedules come from

| Source | Format | Time zone |
|---|---|---|
| BigQuery scheduled query | Data Transfer Service wording (`every day 02:00`) | UTC |
| Cloud Scheduler job that calls the Dataproc API (instantiate a template, submit a job, create a batch) | unix cron, five fields | the scheduler job's own |

A Cloud Scheduler job is matched to the Dataproc definition it starts: a template by
region and id, a job by region, main file and cluster, a batch by region and main file.
If it starts nothing in the plan, it gets its own REPORT row. If two scheduler jobs start
the same definition, neither schedule is carried, because an AIDP job holds only one.
Inventory needs `--scan-services` and **Cloud Scheduler Viewer** on the service account.
Scheduler locations are scanned in the same regions as Dataproc.

## Rules

| Rule | When | Result | Type |
|---|---|---|---|
| `J03_SCHEDULE` | the schedule translates exactly | Quartz on the job, `pauseStatus: PAUSED` | rewrite |
| `J03_SCHEDULE` | `every N hours` / `every N minutes` | placed by the query's next run time, on the assumption that the interval counts from each scheduled start | caveat |
| `J04_ONE_JOB_PER_SCHEDULE` | two Cloud Scheduler jobs start one definition, or a cron restricts both day of month and day of week (`0 9 1 * 1`: the 1st *or* a Monday) | one AIDP job per schedule, named `_1`, `_2`, ...: the cron is split into its day-of-month and day-of-week halves | rewrite |
| `J01_UNSCHEDULED` | the schedule does not translate | job created unscheduled; the source schedule is in the finding | flag |
| `J02_DESTINATION_TABLE` | a scheduled query writes its result into a destination table | no job is created: the inventory does not record the table or the write mode, so the `INSERT` is added by hand | flag |

A blocked job is not created, so it carries no schedule finding.

## Unix cron → Quartz

Quartz adds a seconds field (`0`). Its day-of-week numbering differs from unix cron's
(1 is Sunday in Quartz, 0 or 7 in cron), so days are written as names. Quartz needs `?`
in whichever day field is unused.

| Cron | Quartz |
|---|---|
| `0 2 * * *` | `0 0 2 * * ?` |
| `30 2 * * 1-5` | `0 30 2 ? * MON,TUE,WED,THU,FRI` |
| `0 6 1 * *` | `0 0 6 1 * ?` |
| `0 9 1 * 1` | not translated: cron runs when **either** day field matches; Quartz cannot express that |
| `@daily`, `L`, `FRI-SUN` | not translated |

## BigQuery wording → Quartz (UTC)

| Schedule | Quartz |
|---|---|
| `every day 02:00` | `0 0 2 * * ?` |
| `every monday,friday 09:30` | `0 30 9 ? * MON,FRI` |
| `1,15 of month 04:00` | `0 0 4 1,15 * ?` |
| `every 6 hours`, next run `02:17` | `0 17 2/6 * * ?` (caveat) |
| `every 15 minutes`, next run `02:07` | `0 7/15 * * * ?` (caveat) |
| `every 5 hours` | not translated: does not divide a day, so its times drift |
| `first sunday of quarter 00:00`, anything else | not translated |
