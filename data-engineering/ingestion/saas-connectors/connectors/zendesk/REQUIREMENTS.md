# Zendesk connector: requirements

Status: scope approved 2026-09-30. Task 1 (credentials/session scaffolding)
implemented and reviewed. Task 2 (the spike) is **blocked**: Zendesk has
permanently blocked API-token creation for any account created on/after
2026-07-28 (verified, see `connectors/zendesk/CLAUDE.md`), and all API
tokens stop working everywhere by 2027-04-30 regardless — so F1's plain
Basic-auth design below may need to become an OAuth `client_credentials`
design instead. Tasks 3+ have not started. Every fact below is
**unverified** (from Zendesk's public docs) unless marked otherwise; the
spike (Task 2) must confirm each one before Task 3 is written, same
discipline as the Jira connector.

## Goal

An AIDP user can load tickets from a Zendesk Cloud site into a Spark
DataFrame, either in full or only what changed since the last run, without
working out Zendesk's incremental-export endpoint, cursor paging, or its
much tighter rate limit themselves.

## Prerequisites (gates)

- G1. A live AIDP workspace and running cluster with notebook access.
- G2. A Zendesk trial or sandbox site (free, self-service at
  zendesk.com/register) and an API token for the account (Admin Center →
  Apps and integrations → APIs → Zendesk API).
- G3. Oracle's AIDP connector documentation checked: no native Zendesk type.
  **Partially checked 2026-09-30** — `oracle-aidp-samples` grepped for
  "zendesk", no match. Not yet checked against Oracle's connectors-plugin
  README/blog directly — do that during the spike.

## Functional requirements

- F1. Authenticate with HTTP Basic using a subdomain, email and API token
  supplied by environment variable (or OCI Vault via `_shared/aidp_secrets.py`).
  Credentials are never printed or logged.
- F2. Export tickets via `GET /api/v2/incremental/tickets/cursor.json` with a
  `start_time` (first call) or `cursor` (subsequent calls).
- F3. Page through all matching tickets completely and without skipped or
  duplicated rows, using the response's `after_cursor` only (no offset),
  until `end_of_stream: true`.
- F4. Incremental read: return tickets updated after a supplied watermark.
  **Open question for the spike:** a `start_time` watermark (like Jira's
  `updated >=`) vs. persisting and resuming from the last run's
  `after_cursor` — different mechanics; the spike decides which.
- F5. Retry on HTTP 429, honouring this endpoint's specific rate limit (10
  req/min, 30 with the High-Volume add-on) and its rate-limit response
  headers, with a bounded number of retries. Fail with a clear error
  afterwards.
- F6. Every request has a timeout.
- F7. Return a Spark DataFrame with typed columns for common ticket fields
  (id, subject, status, priority, requester, assignee, group, created_at,
  updated_at, tags); `custom_fields` and any other field not recognised is
  carried as a JSON string (`raw_fields`), same pattern as Jira — but
  Zendesk's `custom_fields` shape (`[{id, value}]` array) is different from
  Jira's `customfield_NNNNN` keys and needs its own tests, not a port of
  Jira's fallback code unchanged.
- F8. Clear errors for authentication failure, an invalid/stale cursor, and
  a site that returns HTML instead of JSON.

## Non-functional requirements

- N1. Runs on Spark 3.5, Python 3.11, Java 17 with no new cluster dependency
  beyond `requests`.
- N2. Unit tests are offline and deterministic.
- N3. No credential, subdomain, or email appears in code, logs, results or
  notebook outputs committed to the repo.
- N4. The example notebook runs top to bottom on a fresh cluster with only
  environment variables set.

## Spike questions (must be answered before the helper is written)

1. Does `GET /api/v2/incremental/tickets/cursor.json` behave as documented
   against a real trial site: `after_cursor` advances correctly,
   `end_of_stream` flips to `true` on the final page?
2. Does resuming a later run from a `start_time` alone give correct
   incremental results, or is persisting `after_cursor` required? (Settles
   F4.)
3. What is the actual rate-limit behaviour and header format on this
   specific endpoint (confirm 10 req/min; confirm the
   `Zendesk-RateLimit-incremental-exports-cursor` header exists as
   documented; does a 429 here include `Retry-After`, or only the custom
   header)?
4. What is the exact shape of `custom_fields`, `requester`/`assignee`
   (person references — IDs or embedded objects?), and the timezone of
   `created_at`/`updated_at`?
5. What does an invalid or expired cursor return (error shape, HTTP status)?
6. Native-type check against Oracle's AIDP connector documentation directly
   (not just the samples repo grep) — see G3.

## Acceptance criteria

- A1. A full read of a trial site's tickets returns the same count as the
  Zendesk UI's ticket list. Not yet checked — needs the spike/build first.
- A2. With a small page size forcing many pages, a full read returns every
  ticket ID exactly once.
- A3. An incremental read returns only tickets updated after the watermark,
  and re-running with the new watermark returns no unseen tickets — exact
  mechanism depends on the F4 spike answer.
- A4. A forced 429 in unit tests results in a bounded retry respecting this
  endpoint's specific limit, then a clear error.
- A5. The example notebook records a dated PASS row in `live-results/`, and
  the gotchas found are written into the connector's README and CLAUDE.md.

## Known limitations to document

- An `updated_at` watermark (or cursor) does not detect hard ticket deletes.
- A free trial site cannot prove behaviour at production ticket volume, and
  its rate-limit tier may differ from a paid plan's.
