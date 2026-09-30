# Jira Cloud connector: requirements

Status: spike complete and verified live (2026-09-29, see `spike/RESULTS.md`);
implementation complete and unit-tested (62 tests); a whole-branch review found
and this document's requirements now reflect two fixes made after that review
(F7's JSON fallback was unimplemented until the fix; F10 below is new). Task 8
(live AIDP run) is still pending — see `live-results/RESULTS.md`.

## Goal

An AIDP user can load issues from a Jira Cloud project into a Spark DataFrame,
either in full or only what changed since a watermark, without working out
Jira's search endpoint, pagination and rate-limit behaviour themselves.

## Prerequisites (gates)

- G1. A live AIDP workspace and running cluster with notebook access.
- G2. A Jira Cloud site (free, self-service at id.atlassian.com) and an API
  token for the account (id.atlassian.com/manage/api-tokens).
- G3. Oracle's AIDP connector documentation checked: no native Jira type. If
  one exists, the deliverable becomes a recipe for it and this file changes.
  **Checked 2026-09-29** against Oracle's connectors plugin README and a web
  search of the `aidataplatform` type list; neither includes Jira. Reasonably
  confident but not certain — Oracle's own blog page on external connectors
  could not be fetched directly to confirm against the canonical source. See
  `spike/RESULTS.md`.

## Functional requirements

- F1. Authenticate with HTTP Basic using a site, email and API token supplied
  by environment variable. Credentials are never printed or logged.
- F2. Search issues via `POST /rest/api/3/search/jql` with a JQL string and a
  field list.
- F3. Page through all matching issues completely and without skipped or
  duplicated rows, using `nextPageToken` only (no offset).
- F4. Incremental read: return issues whose `updated` is after a supplied
  watermark, re-reading a small overlap window. The caller de-duplicates on
  issue key.
- F5. Retry on HTTP 429, honouring `Retry-After` when present, with a bounded
  number of retries. Fail with a clear error afterwards.
- F6. Every request has a timeout.
- F7. Return a Spark DataFrame with typed columns for common issue fields
  (key, summary, status, priority, assignee, reporter, created, updated,
  issuetype, project); any field not recognised is carried as a JSON string.
- F8. A caller-supplied JQL fragment containing `ORDER BY` is rejected, since
  paging supplies its own ordering.
- F9. Clear errors for authentication failure, an unknown project/JQL syntax
  error, and a site that returns HTML instead of JSON.
- F10. The watermark bounds sent in JQL are expressed in the searching
  account's own timezone (`account_timezone()`, from `/rest/api/3/myself`),
  not UTC. **Found necessary by a whole-branch review after the spike** — Jira
  interprets JQL date-time literals in the account's timezone, and a UTC
  literal against a non-UTC account silently skips or delays issues. A caller
  who omits the timezone gets UTC bounds, which is only correct for a UTC
  account.

## Non-functional requirements

- N1. Runs on Spark 3.5, Python 3.11, Java 17 with no new cluster dependency
  beyond `requests`.
- N2. Unit tests are offline and deterministic.
- N3. No credential, site name, or email appears in code, logs, results or
  notebook outputs committed to the repo.
- N4. The example notebook runs top to bottom on a fresh cluster with only
  environment variables set.

## Spike questions (must be answered before the helper is written)

1. Does `POST /rest/api/3/search/jql` behave as documented against a real
   site: JQL + fields in the body, `nextPageToken` in the response?
2. What is the real maximum `maxResults`, and what happens above it?
3. What are the actual rate-limit behaviour and headers?
4. How do custom fields, `assignee`/`reporter` (person objects), and
   timestamps appear in the response, and in what timezone?
5. What does an invalid JQL string return (error shape, HTTP status)?

## Acceptance criteria

- A1. A full read of project `KAN` returns the same issue count as the
  project's own issue count in the Jira UI. **Locally verified** (3-issue test
  project, real Jira, real local Delta write) via
  `spike/local_spark_check.py`; not yet checked on an AIDP cluster.
- A2. With `maxResults` set small enough to need many pages, a full read
  returns every issue key exactly once. **Locally verified**: a real Delta
  table's row count equalled its distinct-key count after the write.
- A3. An incremental read with a watermark returns only issues updated after
  it (plus the overlap window), and re-running with the new watermark returns
  no unseen issues. **Locally verified**: a second run against unchanged data
  read zero issues and left the target's row count unchanged.
- A4. A forced 429 in unit tests results in a bounded retry and then a clear
  error. Verified by offline unit tests.
- A5. A caller JQL string containing `ORDER BY` raises before any request is
  sent. Verified by offline unit tests.
- A6. The example notebook records a dated PASS row in `live-results/`, and
  the gotchas found are written into the connector's README and CLAUDE.md. **Not
  yet done** — needs an actual AIDP cluster; see `LIVE_TEST_GUIDE.md`. A1–A3
  passing locally does not substitute for this: AIDP's credential store,
  cluster networking, and exact runtime versions remain unverified until this
  runs there.

## Known limitations to document

- An `updated` watermark does not detect hard deletes.
- A small personal Jira site cannot prove behaviour at production volume.
