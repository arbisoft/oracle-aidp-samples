# Jira Cloud Connector Samples

`Jira_Cloud.ipynb` reads Jira Cloud issues through the Jira REST API and loads them into a Delta table: a full load on the first run, then incremental loads using the newest `updated` value already in the table, merged on `key`. Read-only against Jira.

The logic lives in the [`aidp-connector-jira`](https://github.com/arbisoft/oracle-aidp-connectors/tree/main/src/aidp_connector_jira) package, unit-tested there; the notebook configures and calls it. That package's README covers every option and the known limits.

## Cluster library

One file, built from that package's folder with `uv build` and installed as a cluster library, then a cluster restart: `aidp_connector_jira-<version>-py3-none-any.whl`. It needs only `requests`, which AIDP clusters already have.

## AIDP notes

Observed on AIDP clusters on 2026-10-01 and 2026-10-05:

- `requests` is already installed; the cluster reaches Jira Cloud over HTTPS.
- Incremental loads work for an account whose Jira timezone is not UTC: Jira reads the query's date-times in the account's timezone, and the package writes them in it.
- An issue edited less than a minute before a run is picked up by the next run: query bounds are rounded to the minute. Nothing is lost.
- The cluster UI had no environment-variable setting, and notebooks cannot prompt for input, so the credentials come from the Credential Store.

## Limits

- An incremental run does not see deleted issues. A full refresh, `run(spark, config, creds, "full")`, reloads the matching issues and overwrites the table.
- Custom fields listed in `extra_fields` land as JSON in the `raw_fields` column, not as typed columns.
- Every issue of a run is held in driver memory; for a very large first load, narrow the query and load in windows.
