# Jira Cloud connector: instructions

Loaded when working in `connectors/jira/`. Project-wide rules are in the root
`CLAUDE.md`; what the connector must do is in `REQUIREMENTS.md`.

## Scope

Read-only ingestion of Jira Cloud issues into a Spark DataFrame from an AIDP
notebook, using the REST API v3. No writes, no webhooks, no OAuth in v1.

## Configuration

`credentials_from_env()` resolves each of the three values below through
`connectors/_shared/aidp_secrets.py`: OCI Vault first (if `OCI_VAULT_ID` is
set), falling back to a plain environment variable. This is structurally
implemented and unit-tested; the Vault path itself has not been exercised
against a real OCI Vault (unverified until a live run proves it).

- `JIRA_SITE`: site host, e.g. `example.atlassian.net` (no scheme, no path)
- `JIRA_EMAIL`: the Atlassian account email
- `JIRA_API_TOKEN`: an API token from https://id.atlassian.com/manage/api-tokens

Never hard-code these, never put them in a notebook cell output, never commit a
real site name. Examples use `<site>.atlassian.net`.

## API facts

**Verified 2026-09-29** against a live site (`spike/RESULTS.md` has the raw
probe output):

- Auth: HTTP Basic, `email:api_token`. A real API token from
  id.atlassian.com/manage/api-tokens was accepted with no MFA/Basic-Auth
  restriction of any kind, on the first try.
- `POST /rest/api/3/search/jql` behaves exactly as documented: `nextPageToken`
  advances correctly, `isLast` flips to `true` on the final page (and
  `nextPageToken` is then absent), and paging is lossless.
- **`maxResults` maximum is exactly 5000.** A value above it is rejected with
  `HTTP 400 {"errorMessages":["The max results parameter has to be between 1
  and 5,000."]}` — not silently capped.
- **A JQL query with no restricting clause is rejected**: `HTTP 400
  {"errorMessages":["Unbounded JQL queries are not allowed here..."]}`. Does
  not affect this connector, since `search_issues` always includes an
  `updated <= "<until>"` bound — but never call `search_issues()` with no
  `query` and no watermark expecting an unrestricted default; there isn't one.
- **Timestamps are NOT UTC.** `updated`/`created` come back in the requesting
  account's configured timezone (observed `+0500`, not `+0000`). The parser
  (`%Y-%m-%dT%H:%M:%S.%f%z`) handles any offset correctly — no code change was
  needed — but do not assume UTC when reading these columns downstream.
- Rate-limit headers `X-Ratelimit-Limit` / `X-Ratelimit-Remaining` are present
  and decrement per request (200 → 197 observed). No 429 was ever triggered
  live, so the retry path is verified only by offline unit tests.
- `reporter`/`assignee` are dicts with `displayName` (or `null` when unset);
  `status`/`priority` are dicts with `name`. Custom fields follow
  `customfield_NNNNN`.
- **Custom fields are preserved, not dropped.** Any requested field not in
  `TYPED_FIELDS` (including a `customfield_NNNNN`) lands in a trailing
  `raw_fields` JSON-string column rather than being silently discarded (fixed
  after an initial version of this connector dropped them — see the whole-
  branch review notes in the implementation plan).
- The older `GET/POST /rest/api/3/search` (offset/`startAt` pagination) was
  fully sunset by Atlassian by 31 October 2025 and no longer works.

## Working rules

- Use `requests` only. Do not add the Atlassian Python SDK.
- Page with `nextPageToken` only. Never assume or compute an offset.
- Incremental loads use JQL `updated >= "<timestamp>" ORDER BY updated ASC, key
  ASC` inside a fixed-until window: fix an upper bound per run, re-read a small
  overlap on the next run's lower bound, de-duplicate by issue key.
- Reject a caller-supplied JQL fragment that already contains `ORDER BY` —
  paging adds its own ordering, and two `ORDER BY` clauses conflict.
- Retry on 429 with `Retry-After`, bounded. Every request has a timeout.
- Unit tests use mocked HTTP. Never call a real site from a unit test.
- Live tests force a small `maxResults` so paging is exercised.

## Shared package (`connectors/_shared/`)

`jira.py` imports two sibling modules rather than defining this logic itself:

- `aidp_http.py`: the bounded-429-retry request engine (`post_json`,
  `get_json`), the generic `ConnectorError`/`ConnectorAuthError`/
  `ConnectorRateLimitError` classes (re-exported here as `JiraError` etc. —
  same classes, not subclasses), and `redact()`.
- `aidp_secrets.py`: the Vault → environment → default credential fallback
  used by `credentials_from_env()`.

Both are adapted from Oracle's own
`oracle-ai-data-platform-workbench-spark-connectors` plugin (MIT licence,
Copyright (c) 2026 Ahmed Awan) — rewritten for this repo's naming and test
style, not copied verbatim; see each file's module docstring for the
attribution. `aidp_jars.py` (runtime JAR/JDBC-driver loading) is adapted the
same way, ready for the next connector that needs one (Jira does not).

A notebook must upload **both** `jira.py` and the `_shared/` folder to the
same workspace path — see the example notebook's first cell.

## Files (created once the plan is approved)

```
connectors/jira/
  CLAUDE.md          this file
  REQUIREMENTS.md    what it must do, acceptance criteria
  README.md          setup, usage, gotchas (written from live results)
  jira.py            helper module (imports connectors/_shared)
  tests/             offline unit tests
  examples/          notebooks
  live-results/      dated result rows (placeholders only, no real site name)
```
