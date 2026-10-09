"""Source schedules → AIDP Quartz cron, exact or not at all.

AIDP jobs take `schedule{quartzCronExpression, timezoneId, pauseStatus}`; migrated jobs
are always created PAUSED (live on AIDP, 2026-10-09: a paused every-minute job started
no run). Two source formats:

  * Cloud Scheduler (which starts Dataproc templates and jobs): unix cron, five fields,
    in the job's own time zone.
  * BigQuery scheduled queries: the Data Transfer Service wording, always UTC:
    `every day 02:00`, `every monday,friday 09:30`, `1,15 of month 04:00`, and
    `every N hours` / `every N minutes`, whose phase comes from the next run time.

Anything else returns the reason it was not translated; the job is created unscheduled.
"""
from __future__ import annotations

import re

_DAYS = ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")
_NUMBERS = re.compile(r"[0-9*,/-]+")
_NAMED = re.compile(r"[0-9A-Za-z*,/-]+")  # month and day of week may use names


def _dow(field: str) -> list[int] | None:
    """A unix-cron day-of-week field → the days it names (0 = Sunday), or None if unreadable."""
    def day(v: str) -> int | None:
        if v.isdigit() and int(v) <= 7:
            return int(v) % 7
        return _DAYS.index(v.upper()) if v.upper() in _DAYS else None

    out: set[int] = set()
    for part in field.split(","):
        span, _, step = part.partition("/")
        if step and not step.isdigit() or step == "0":
            return None
        if span == "*":
            lo, hi = 0, 6
        else:
            a, _, b = span.partition("-")
            lo, hi = day(a), (day(b) if b else (6 if step else None))
            if lo is None or (b and hi is None):
                return None
            if hi is None:
                hi = lo
            if b and b.isdigit() and int(b) == 7:
                hi = 7  # 5-7 is Friday to Sunday
            if hi < lo:
                return None
        out.update(d % 7 for d in range(lo, hi + 1, int(step or 1)))
    return sorted(out)


def from_unix_cron(expr: str) -> tuple[str | None, str]:
    """`30 2 * * 1-5` → (`0 30 2 ? * MON,TUE,WED,THU,FRI`, ""), or (None, why)."""
    fields = (expr or "").split()
    if len(fields) != 5 or not all((_NAMED if i > 2 else _NUMBERS).fullmatch(f) for i, f in enumerate(fields)):
        return None, f"{expr!r} is not five unix cron fields of numbers, ranges, lists and steps"
    minute, hour, dom, month, dow = fields
    if dom != "*" and dow != "*":  # cron runs when either matches; Quartz cannot say that
        return None, f"{expr!r} restricts both day of month and day of week, which Quartz cannot express"
    if dow == "*":
        return f"0 {minute} {hour} {dom} {month.upper()} ?", ""
    days = _dow(dow)
    if not days:
        return None, f"day of week {dow!r} not understood"
    return f"0 {minute} {hour} ? {month.upper()} {','.join(_DAYS[d] for d in days)}", ""


_TIME = r"(\d{1,2}):(\d{2})"
_NAMES = {d.lower(): d for d in _DAYS} | {n: d for d, n in zip(_DAYS, (
    "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"))}


def from_bigquery(expr: str, next_run: str | None = None) -> tuple[str | None, str]:
    """A scheduled query's schedule (UTC) → (Quartz, ""), or (None, why). An interval comes back
    as (Quartz, caveat): it is placed by the next run time, on the assumption that the interval
    counts from each scheduled start."""
    s = " ".join((expr or "").lower().split())
    if m := re.fullmatch(rf"every day {_TIME}", s):
        return f"0 {int(m[2])} {int(m[1])} * * ?", ""
    if m := re.fullmatch(rf"every ([a-z]+(?:,[a-z]+)*) {_TIME}", s):
        if all(d in _NAMES for d in m[1].split(",")):
            days = sorted({_NAMES[d] for d in m[1].split(",")}, key=_DAYS.index)
            return f"0 {int(m[3])} {int(m[2])} ? * {','.join(days)}", ""
    if m := re.fullmatch(rf"(\d{{1,2}}(?:,\d{{1,2}})*) of month {_TIME}", s):
        return f"0 {int(m[3])} {int(m[2])} {','.join(str(int(d)) for d in m[1].split(','))} * ?", ""
    if m := re.fullmatch(r"every (\d+) (minutes|hours)", s):
        n, unit = int(m[1]), m[2]
        t = re.search(r"T(\d{2}):(\d{2})", next_run or "")
        if not t:
            return None, f"{expr!r} repeats from when it was created; no next run time to place it by"
        hh, mm = int(t[1]), int(t[2])
        if unit == "minutes" and 0 < n < 60 and 60 % n == 0:
            return f"0 {mm % n}/{n} * * * ?", f"placed by the next run time {next_run}"
        if unit == "hours" and 0 < n <= 24 and 24 % n == 0:
            return (f"0 {mm} {hh % n if n < 24 else hh}{f'/{n}' if n < 24 else ''} * * ?",
                    f"placed by the next run time {next_run}")
        return None, (f"{expr!r}: the interval does not divide {'a day' if unit == 'hours' else 'an hour'}, so its "
                      "times drift, which Quartz cannot express")
    return None, f"{expr!r} is not a schedule form translated exactly"


if __name__ == "__main__":
    assert from_unix_cron("30 2 * * 1-5")[0] == "0 30 2 ? * MON,TUE,WED,THU,FRI"
    assert from_bigquery("every 24 hours", "2026-10-10T02:17:00Z")[0] == "0 17 2 * * ?"
    print("ok")
