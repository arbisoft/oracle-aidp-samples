# HubSpot Connector Sample

`HubSpot.ipynb` ingests HubSpot CRM contacts, companies, deals and the associations between them into Delta tables with the [HubSpot CRM API](https://developers.hubspot.com/docs/api/crm/understanding-the-crm). AIDP has no built-in HubSpot connector type, so the notebook uses the `aidp-connector-hubspot` Python package. The source code and the tests are in [oracle-aidp-connectors](https://github.com/arbisoft/oracle-aidp-connectors), folder `src/aidp_connector_hubspot`.

## Prerequisites

- A HubSpot service key or private app token with the read scopes for contacts, companies and deals, stored as a **Secret Token** in the Credential Store.
- Outbound HTTPS access to `api.hubapi.com` and an existing target catalog.
- The `aidp-connector-hubspot` wheel on the cluster. Build it with `uv build --wheel` in `src/aidp_connector_hubspot` of the connectors repo, upload it to your workspace and add it from the cluster **Library** tab. The notebook also has a commented `%pip install` line for a notebook-scoped install.
- A `hubspot_ingest.yaml` config in your workspace. The notebook shows the minimum content. The connectors repo has the full sample.

## How to run

Set the placeholders in the notebook and run the cells in order. The first run is a full load. Later runs read only what changed. The tables are `contacts`, `companies`, `deals`, `associations` and `hubspot_sync_state` in `<CATALOG>.<SCHEMA>`. Run one instance at a time.

## Known limits

- HubSpot search is eventually consistent. An edit indexed after the overlap window is missed until the next full run.
- GDPR-erased contacts and records archived more than 90 days ago are never flagged by an incremental run. Only a full run clears them.
- The table schemas are fixed. After a schema change, drop the tables and run a full load.
- CRM data holds personal data. The tables inherit the catalog's access controls.

The connectors repo lists all limits.

## Validation status

The packaged version (`aidp-connector-hubspot` 0.1.0) ran live on AIDP on 2026-10-02 through the notebook in the connectors repo, against a small HubSpot test account. The runs were a first load, a rerun with no changes, a deal rename, a deal delete and a full refresh. Unit tests run in the connectors repo.

This sample notebook also ran live on AIDP on 2026-10-02, in a new schema on the shared cluster. It installed the wheel, loaded 14 contacts, 7 companies, 45 deals and 107 associations, read 0 changed rows on an incremental rerun and finished a full refresh with the same counts. Not run live in any form: merged contacts, HubSpot's real `429` rate limit, a scheduled job, accounts with many thousands of records and a non-empty deal currency.
