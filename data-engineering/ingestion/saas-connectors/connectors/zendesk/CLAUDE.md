# Zendesk connector: instructions

Loaded when working in `connectors/zendesk/`. Project-wide rules are in the
root `CLAUDE.md`; what the connector must do is in `REQUIREMENTS.md`.

Status: scope approved 2026-09-30. Task 1 (credentials/session scaffolding,
`zendesk.py`) implemented and reviewed on branch `zendesk-connector`. Task 2
(the live spike) is **blocked** — see the API-token finding below — pending
a decision on whether to test against an older Zendesk account or redesign
for OAuth. Tasks 3+ have not started.

## Scope

Read-only ingestion of Zendesk tickets into a Spark DataFrame from an AIDP
notebook, using the REST API v2 Incremental Exports endpoint. No writes, no
webhooks.

**"No OAuth2 in v1" is no longer a settled scope decision — see the
API-token finding below.** It was written assuming plain API-token Basic
auth would always be available, the way it is for Jira. That assumption is
now known to be false for any Zendesk account created on/after
2026-07-28, so this repo's scope may need to change to require OAuth
instead of merely excluding it. Do not build Tasks 3+ on the old
"API token always available" assumption until this is resolved.

## Configuration

`credentials_from_env()` (planned, same as Jira's) resolves each value below
through `connectors/_shared/aidp_secrets.py`: OCI Vault first (if
`OCI_VAULT_ID` is set), falling back to a plain environment variable.

- `ZENDESK_SUBDOMAIN`: subdomain only, e.g. `example` for
  `example.zendesk.com` (no scheme, no path)
- `ZENDESK_EMAIL`: the Zendesk account email
- `ZENDESK_API_TOKEN`: an API token from Admin Center → Apps and
  integrations → APIs → Zendesk API — **see the finding below: this may not
  be obtainable at all, depending on the account's creation date.**

Never hard-code these, never put them in a notebook cell output, never
commit a real subdomain. Examples use `<subdomain>.zendesk.com`.

## Verified finding, 2026-09-30: API tokens are blocked for new accounts

**Verified** against Zendesk's own support article (not from memory or a
blog): [Announcing the removal of API tokens as an authentication method](https://support.zendesk.com/hc/en-us/articles/10851263566234-Announcing-the-removal-of-API-tokens-as-an-authentication-method-for-API-requests).

- "Accounts created on and after July 28, 2026, cannot create or use API
  tokens." No trial/paid distinction.
- Independently of account age, "all remaining API tokens will be
  deactivated permanently" on April 30, 2027 — Basic-auth-via-API-token
  stops working for everyone by then regardless.
- Required replacement: OAuth, via an OAuth client registered in Admin
  Center, using either the `client_credentials` or `authorization_code`
  grant. `client_credentials` (server-to-server, no interactive login) is
  the fit for an AIDP notebook, not `authorization_code` — unverified
  which scopes/permissions it needs for the Incremental Exports endpoint;
  that's now part of what Task 2's spike must answer if this connector
  moves to OAuth.
- Discovered live: a fresh trial Zendesk account (created 2026-09-30, after
  the cutoff) has no "Add API token" control anywhere in Admin Center's API
  tokens page, even as an Owner/Admin — not a permissions or UI issue.

## API facts — ALL UNVERIFIED until the spike runs

From Zendesk's public docs, not yet observed live. Don't treat any of this
as fact in a shipped README until `spike/RESULTS.md` confirms it.

- Auth: HTTP Basic, `{email}/token:{api_token}`, base64-encoded — **only
  obtainable on an account created before 2026-07-28; see the verified
  finding above.** Admin Center must also have API token access enabled.
- `GET /api/v2/incremental/tickets/cursor.json`: first call takes
  `start_time` (Unix epoch); each page returns `after_cursor` and
  `end_of_stream`; `per_page` up to 1000, default 1000.
- **Rate limit: 10 req/min on this endpoint (30 with the High-Volume
  add-on)** — far tighter than Jira's. Headers reportedly
  `Zendesk-RateLimit-incremental-exports-cursor: total={n}; remaining={n};
  resets={n}`, not Jira's generic `X-Ratelimit-*`. Confirm the header name
  and whether `Retry-After` also appears on a 429.
- `custom_fields`: documented as `[{id, value}]` — an array, not named keys
  like Jira's `customfield_NNNNN`. Needs its own `to_dataframe` test.
- Timestamps: docs suggest ISO 8601 with an explicit UTC offset, no known
  account-timezone quirk like Jira's — confirm live regardless, since
  Jira's own "no issue" assumption here was wrong until a review caught it.
- Sources: [Incremental Exports](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/),
  [Rate limits](https://developer.zendesk.com/api-reference/introduction/rate-limits/).

## Working rules

- Use `requests` only. Page with `after_cursor` only, never an offset.
- Don't implement incremental reads until the spike settles
  REQUIREMENTS.md's F4 (timestamp watermark vs. persisted cursor).
- Retry on 429 per this endpoint's actual rate limit and header format,
  confirmed by the spike, not assumed from Jira's. Every request has a
  timeout.
- Unit tests use mocked HTTP, never a real site. Live tests force a small
  `per_page` so paging is exercised.

## Shared package (`connectors/_shared/`)

`zendesk.py` (Task 1 done) reuses Jira's sibling modules unchanged so far,
and will continue to unless the spike or an OAuth pivot says otherwise:

- `aidp_http.py`: retry engine + `redact()`. May need a Zendesk-specific
  rate-limit header parser layered on top — decide after the spike.
- `aidp_secrets.py`: Vault → environment → default credential fallback,
  same shape as Jira (subdomain/email/token instead of site/email/token).
  If this connector moves to OAuth, this file may need an OAuth-token
  variant added — a genuinely new need, not yet built anywhere in this repo.

`aidp_jars.py` not needed — Zendesk is REST, no JDBC driver.

A notebook must upload **both** `zendesk.py` and the `_shared/` folder to
the same workspace path, exactly like Jira's notebook does.

## Files

```
connectors/zendesk/
  CLAUDE.md          this file
  REQUIREMENTS.md    what it must do, acceptance criteria
  README.md          setup, usage, gotchas (written from live results) — not yet created
  zendesk.py         helper module (imports connectors/_shared) — Task 1 only so far
  tests/             offline unit tests — Task 1's tests only so far
  spike/             throwaway probe code — written, not yet run (blocked, see above)
  examples/          notebooks — not yet created
  live-results/      dated result rows — not yet created
```
