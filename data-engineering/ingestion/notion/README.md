# Notion Ingestion (Full Refresh and CDC)

Ingest Notion pages, data sources, blocks and users into Delta tables on Oracle AI Data Platform (AIDP) Workbench through the [Notion REST API](https://developers.notion.com/reference/intro). Which objects to ingest, the target catalog and schema, and the refresh mode all come from one YAML file.

This is Python that runs in a notebook or job. It is not a `type` of the built-in `aidataplatform` Spark format.

## What you get

One Delta table per object in `<catalog>.<schema>`:

| Table | Key | Typed columns (plus `raw_json`, `_ingested_at`) |
|---|---|---|
| `pages` | `id` | `created_time`, `last_edited_time`, `created_by_id`, `last_edited_by_id`, `in_trash`, `parent_type`, `parent_id`, `url`, `public_url`, `title` |
| `data_sources` | `id` | same common columns, `url`, `title` |
| `blocks` | `page_id`, `id` | same common columns, `type`, `has_children`, `page_id`, `depth`, `position` |
| `users` | `id` | `type`, `name`, `avatar_url`, `email` |
| `notion_sync_state` | `object_name` | `watermark`, `last_mode`, `last_status`, `last_rows`, `last_run_at`, `last_error` |

`raw_json` holds the complete API object, so page properties and block content can be extracted downstream with `get_json_object` or `from_json`.

## Refresh modes

| Object | `cdc` (default) | `full` |
|---|---|---|
| `pages`, `data_sources` | Reads objects edited since the last watermark and merges them on `id`. Trashed objects are picked up and flagged `in_trash`. | Reloads everything and overwrites the table. |
| `blocks` | Re-reads the block tree of each changed page and replaces that page's rows, so deleted blocks disappear. Blocks of trashed pages are removed. | Walks every page and overwrites the table. |
| `users` | Always a full overwrite: Notion users carry no edit time. | Same. |

The first CDC run of an object has no watermark and behaves as a full load. A watermark moves forward only after the table write succeeds, so a failed run is retried over the same window.

## Setup

1. **Create a Notion integration.** In Notion, create an internal integration with *Read content* and, if you ingest `users`, *Read user information*. Copy its secret.
2. **Share content with it.** The API only returns pages and databases that have been shared with the integration.
3. **Store the secret.** Add it to the AIDP Credential Store and note the credential name. For a local run, export it as `NOTION_TOKEN` instead.
4. **Install libraries.** Install `requirements.txt` (`requests`, `pyyaml`) from the cluster **Library** tab and restart the cluster.
5. **Upload this folder** to your workspace.
6. **Configure.** Copy `notion_ingest.sample.yaml` to `notion_ingest.yaml` and set `target.catalog`, `target.schema` and `notion.credential_name`. The catalog must exist; the schema is created if missing.
7. **Run** `notion_ingest.ipynb` after setting `SAMPLE_DIR` in its first code cell.

To schedule it, create a job on the notebook. A job parameter `MODE` set to `full` forces a full refresh for that run, which is useful for a weekly purge alongside frequent CDC runs.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `notion.credential_name` | none | Credential Store entry holding the token. |
| `notion.credential_key` | `secret` | Key inside that credential. |
| `notion.token_env` | `NOTION_TOKEN` | Environment variable used when no credential is named. |
| `notion.api_version` | `2026-03-11` | Value of the `Notion-Version` header. |
| `notion.requests_per_second` | `3` | Client-side throttle. |
| `target.catalog` | required | Target catalog. |
| `target.schema` | required | Target schema. |
| `target.table_prefix` | empty | Prefix for every table name. |
| `sync.mode` | `cdc` | `cdc` or `full`. |
| `sync.objects` | required | Any of `pages`, `data_sources`, `blocks`, `users`. |
| `sync.overlap_seconds` | `120` | How far before the watermark CDC re-reads. |

Unknown keys and unknown object names are rejected, so a typo fails fast instead of being ignored.

## Known limits

- **Speed.** Requests run on the Spark driver, one at a time, within Notion's rate limit. Blocks need at least one request per page: a first load of 10,000 pages takes roughly an hour at 3 requests/second. There is no checkpoint inside an object: if a load fails part-way, the next run starts that object again. Server errors and dropped connections are retried for about a minute and a half before a request gives up.
- **Deletes.** CDC sees trashing, but not permanent deletion or content that was un-shared from the integration. Run a full refresh periodically to purge.
- **Users.** No change tracking, and Notion's user list does not include guests.
- **Comments** are not ingested.
- **Search lag.** Notion search is eventually consistent. An edit made shortly before a run arrives on the next run, as long as it is indexed within `sync.overlap_seconds` of any newer edit the connector has already seen. An edit indexed later than that is missed by CDC and only picked up by a full refresh. Raise `overlap_seconds` if you see this (each run then re-reads more), and schedule a periodic full refresh.
- **One run at a time.** Do not let two runs for the same target overlap: set the job's concurrency to 1. Staging tables are per run, but both runs would still write the same target tables and watermarks.
- **Unreadable nested blocks.** If Notion returns 403 or 404 for the children of a block inside a readable page, that block is kept and its children are skipped.
- **Block replacement** in CDC is a delete followed by an insert. If a run dies between the two, the watermark is not advanced and the next run repeats it.
- **Synced blocks.** The original synced block's children are ingested; references to it are recorded without their children.
- **Schema changes.** Table schemas are fixed. If a later version adds a column, drop the tables and run a full refresh.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install pytest requests pyyaml
.venv/bin/python -m pytest tests -q
```

The unit tests need no network and no Spark. They cover configuration, the HTTP client, extraction, row mapping, the generated SQL, watermark handling and the full/CDC logic, using an in-memory Notion and in-memory tables.

**Validation status:** unit-tested, and the full/CDC flow (first load, edit, trash, no-change, full refresh) has been run against local open-source Spark 3.5.3 with Delta Lake 3.2.1 using a simulated Notion API. It has not yet been run on an AIDP cluster or against a live Notion workspace.
