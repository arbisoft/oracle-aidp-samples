# Jira Cloud Connector Samples

`Jira_Cloud.ipynb` reads Jira Cloud issues through the Jira REST API and loads them into a Delta table: a full load on the first run, then incremental loads using the newest `updated` value already in the table, merged on `key`. Read-only against Jira. It needs no extra library: `requests` is already on the AIDP cluster.

## Setup

1. Create an API token at https://id.atlassian.com/manage/api-tokens.
2. In the AIDP Credential Store, create a **Secret Token** credential with keys `site`, your `<site>.atlassian.net` host, `email` and `token`.
3. Open the notebook, fill in the Configuration cell, and run it top to bottom.

## AIDP notes

Observed on 2026-10-01 on an AIDP cluster:

- This notebook ran top to bottom against a real Jira Cloud site: the first run loaded 4 issues into a new Delta table, matching Jira's count. After one issue was added in Jira, the incremental load read 2 issues, the new one and one inside the overlap, and the table held 5.
- `requests` is already installed; the cluster reaches Jira Cloud over HTTPS.
- The cluster UI had no environment-variable setting, and notebooks cannot prompt for input, so the credentials come from the Credential Store.
- An issue edited less than a minute before a run is picked up by the next run: query bounds are rounded to the minute. Nothing is lost.

## Limits

- The watermark does not see deleted issues.
- Only the typed fields are loaded: key, summary, status, priority, assignee, reporter, issue type, project, created and updated. Custom fields are not.
- Every issue of a run is held in driver memory; for a very large first load, narrow `JQL` and load in windows.
- The ORDER BY check is a plain text match, so a query with the words "order by" inside a quoted string is rejected.
- Around a daylight-saving change, the minute-precision bounds in the account's timezone can shift by the ambiguous hour; the default 300-second overlap covers normal cases only.
