# SaaS REST Connectors for AIDP

Read-only Python connectors that load data from SaaS REST APIs into Delta tables on
Oracle AI Data Platform (AIDP) Workbench. Each connector is a single helper module you
upload to a workspace folder and call from a notebook.

| Connector | Status | Notes |
|---|---|---|
| [Jira Cloud](connectors/jira/README.md) | Live-tested | Full then incremental issue load via the `/search/jql` API; example notebook in `connectors/jira/examples/`. |
| [Zendesk](connectors/zendesk/CLAUDE.md) | In progress | Session/auth layer only; blocked on API-token availability for new Zendesk accounts (see `connectors/zendesk/CLAUDE.md`). |

## Layout

```
saas-connectors/
├── connectors/
│   ├── _shared/    # HTTP retry engine, OCI Vault / env credential resolution, runtime jar loading
│   ├── jira/
│   └── zendesk/
├── pytest.ini
└── requirements-dev.txt
```

Connector modules import `aidp_http` / `aidp_secrets` as bare names, so upload the
connector module and `_shared/` together and put both folders on `sys.path`
(the example notebook shows how).

## Tests

Unit tests use fakes and need no network or credentials:

```bash
cd data-engineering/ingestion/saas-connectors
pip install -r requirements-dev.txt
pytest -q
```
