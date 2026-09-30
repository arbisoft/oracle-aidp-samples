# Jira Cloud API spike results

Date: 2026-09-29 · Site: `<site>.atlassian.net` (placeholder — real host never
recorded here or anywhere else in this repo) · Run from: laptop

## G0 (repo-wide gate) — native AIDP connector type check

**Checked 2026-09-29.** Oracle's `oracle-ai-data-platform-workbench-spark-connectors`
plugin README lists 26 `aidataplatform`/native connector types (Oracle/OCI
sources, external RDBMS, SaaS: Salesforce, NetSuite, Snowflake, and
multi-cloud/escape-hatch connectors). Jira is not among them. A general web
search summarizing the same `aidataplatform` type list agrees. Oracle's own
blog post on "bringing external data into AI Data Platform Workbench" could
not be fetched directly (HTTP 403) to double-check against the canonical
source. **Confidence: reasonably high but not certain** — this is two
secondary sources agreeing, not a direct read of Oracle's documentation.
Decision: proceed with the REST-based connector as designed; if a native type
ever surfaces, revisit per the spec's stated rule (native type wins).

## Q1 search/jql shape and paging

Probe output (page size 3, 13 total issues, bounded query
`updated >= "2000-01-01 00:00" ORDER BY updated ASC`):

```
Q1 page=1 keys=3 isLast=False has_token=True
Q1 page=2 keys=3 isLast=False has_token=True
Q1 page=3 keys=3 isLast=False has_token=True
Q1 page=4 keys=3 isLast=False has_token=True
Q1 page=5 keys=1 isLast=True has_token=False
Q1 total=13 unique=13
```

**Verified fact:** `POST /rest/api/3/search/jql` behaves exactly as documented.
`nextPageToken` advances correctly, `isLast` flips to `true` on the final page
and `nextPageToken` is absent there, and paging is lossless (13 total, 13
unique, in the order returned).

**New finding, not in the original design:** a JQL query with no restricting
clause is rejected outright: `HTTP 400 {"errorMessages":["Unbounded JQL
queries are not allowed here. Please add a search restriction to your
query."]}`. This does not affect the shipped connector, because
`search_issues` always includes an `updated <= "<until>"` bound (`until`
defaults to the current time, never omitted from the JQL) — but the fact is
recorded so nobody later builds a bare `search_issues()` call assuming an
unrestricted default query works.

## Q2 maxResults limit and rate-limit headers

```
Q2 requested=5000 status=200 body={"issues":[...
Q2 requested=5001 status=400 body={"errorMessages":["The max results parameter has to be between 1 and 5,000."],"errors":{}}
Q2 headers: [('X-Ratelimit-Limit', '200'), ('X-Ratelimit-Remaining', '197')]
```

**Verified fact:** the maximum `maxResults` is exactly **5000**. A value above
it is rejected with a precise, named error — not silently capped. This
matches the `MAX_PAGE_SIZE = 5000` already used in the plan; no code change
needed. Rate-limit headers `X-Ratelimit-Limit` / `X-Ratelimit-Remaining` are
present and decrement per request (observed 200 → 197 across a handful of
calls). No `Retry-After` header was observed on a 200 response, as expected;
this site was never actually rate-limited during testing, so the 429 path
itself (`post_json`'s retry loop) remains verified only by the offline unit
tests, not by a live 429.

## Q3 field shapes and timestamp timezone

```
Q3 field shapes: {'reporter': ('dict', ['accountId', 'accountType', 'active', 'avatarUrls', 'displayName', 'emailAddress', 'self', 'timeZone']), 'assignee': ('NoneType', None), 'priority': ('NoneType', None), 'updated': ('str', None), 'created': ('str', None), 'status': ('dict', ['description', 'iconUrl', 'id', 'name', 'self', 'statusCategory'])}
Q3 updated raw value: 2026-09-28T19:32:13.094+0500
Q3 utc_now: 2026-09-29T04:46:16
```

**Verified facts:**
- `reporter` is a dict with `displayName` (and other keys we don't use).
  `assignee` and `priority` were `null` on this issue (unassigned/unset) —
  our null-check-first normalization handles this correctly regardless of
  field type.
- `status` is a dict with `name` (matches the design's assumption).
- **Timestamps are NOT UTC.** `updated` came back as
  `2026-09-28T19:32:13.094+0500` — a +05:00 offset, not `+0000`. The format
  `%Y-%m-%dT%H:%M:%S.%f%z` parses this correctly regardless of which offset is
  present, so *reading* a timestamp needed no code change.

  **Correction (post-review, 2026-09-29): the original note that "no code
  change is required" was wrong and has been fixed.** It only checked how
  Jira *returns* timestamps; it never checked how Jira *interprets* the JQL
  date-time literals `search_issues` *sends* for the watermark bounds. Jira
  compares those literals in the searching account's own timezone (per
  `/rest/api/3/myself`'s `timeZone` field), not UTC. Sending a UTC-formatted
  bound to an account not on UTC silently shifts the effective watermark by
  the account's offset — for an account ahead of UTC this drops the most
  recent hours from a full load; for an account behind UTC this **silently
  and permanently skips** issues updated in the gap. Fixed: `jira.py` now has
  `account_timezone()`, and `format_jql_timestamp`/`build_jql`/`search_issues`
  take an explicit `tz`/`tz_name` parameter that the notebook fetches and
  passes on every run. See `CLAUDE.md` and the design spec for the corrected
  description.

## Q4 invalid JQL

HTTP status: 400. Body:
```
{"errorMessages":["Error in the JQL Query: Expecting either 'OR' or 'AND' but got 'jql'. (line 1, character 19)"],"errors":{}}
```

**Verified fact:** an invalid JQL string returns HTTP 400 with a JSON body
containing `errorMessages` (an array of human-readable strings). This matches
`post_json`'s existing error handling, which includes the response body text
in the raised `JiraError` for any 4xx/5xx status — no code change needed.

## Q5 custom fields

Custom field key pattern confirmed: `customfield_NNNNN` (5-digit numeric ID).
Sample from this site: 6 custom fields present (keys withheld here since they
are specific to the test site; the pattern is what matters). The shipped
connector does not surface custom fields in a typed column (`TYPED_FIELDS`
covers only common fields); a caller who needs one passes its
`customfield_NNNNN` key via the `fields` argument to `search_issues`. As of
the post-review fix, any requested field not in `TYPED_FIELDS` — including
custom fields — is preserved in a `raw_fields` JSON-string column rather than
silently dropped (see `CLAUDE.md`).

## Decision

**Proceed: search/jql pagination verified.** Two defects were found after this
spike by a whole-branch code review, both since fixed: (1) the shipped
notebook only requested `key`+`updated`, leaving every other typed column
null — `search_issues` now defaults to every `TYPED_FIELDS` name when the
caller passes no `fields=`; (2) the timezone finding above. See the design
spec's Jira Cloud connector section for the corrected, current description.
