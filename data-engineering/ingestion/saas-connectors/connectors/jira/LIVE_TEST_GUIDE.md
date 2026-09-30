# Jira Cloud connector: live AIDP test guide

For whoever has hands-on AIDP access. This walks through the one thing left
before this connector can be marked supported: a real run on a live AIDP
workspace. Everything else (62+ unit tests, a live-verified spike against
Jira, and a local Spark+Delta rehearsal — see `TESTING.md`) is already done.

Repo: `oracle-aidp-connectors`. Read `CLAUDE.md` at the repo root once before
touching anything — it has the rules this guide assumes (never commit a real
hostname, instance URL, OCID, or credential).

## What you need before starting

- An active AIDP workspace with a running compute cluster and notebook access.
- A Jira Cloud site to read from. Either:
  - your own site (a project like `KAN` with a few issues in it), or
  - a fresh free one — sign up at `id.atlassian.com`, it creates a sample
    "KAN" project automatically.
- A Jira API token: `id.atlassian.com/manage/api-tokens` → **Create API
  token**. Copy it immediately; it's shown once.

## Step 1 — Upload the code

Upload these two things to the **same** folder in the AIDP workspace (e.g.
via the workspace file browser), so it ends up looking like this:

```
<the folder you uploaded to>/
  jira.py
  _shared/
    __init__.py
    aidp_http.py
    aidp_secrets.py
    aidp_jars.py
```

- `connectors/jira/jira.py`
- the whole `connectors/_shared/` folder, **keeping its name** (`_shared`) —
  don't flatten it into the parent folder.

`jira.py` does `import aidp_http` and `import aidp_secrets` as bare names, not
`from _shared import ...` — so it's not enough to put `_shared/` next to
`jira.py`. The folder that *directly contains* `aidp_http.py` and
`aidp_secrets.py` (i.e. `<HELPER_DIR>/_shared`) has to be on `sys.path` too.
The example notebook's first cell already does this (it inserts both
`HELPER_DIR` and `HELPER_DIR/_shared`) — just make sure your upload matches
the layout above so that second path actually contains the files.

Note the workspace path you uploaded them to — you'll need it in Step 3.

## Step 2 — Set credentials

Set three environment variables for the cluster (through cluster environment
settings, or a notebook-scoped `%env` cell if that's what your AIDP setup
allows):

```
JIRA_SITE=<your-site>.atlassian.net
JIRA_EMAIL=<your Atlassian account email>
JIRA_API_TOKEN=<the API token from Step 0>
```

Never type these into a notebook cell that gets saved — set them as
environment variables, not as Python literals.

**Bonus, if you have time:** this connector also supports OCI Vault-backed
credentials (`connectors/_shared/aidp_secrets.py`) — if AIDP's credential
store is easy to reach, storing the token there instead and setting
`OCI_VAULT_ID` tests a path nobody has verified live yet. Not required; plain
environment variables are the documented path.

## Step 3 — Run the notebook

1. Open `connectors/jira/examples/jira_issue_load.ipynb` in the AIDP notebook
   UI (upload it there too, or open it directly if your AIDP setup can pull
   from this repo).
2. In the first cell, set `HELPER_DIR` to wherever you uploaded `jira.py` in
   Step 1. Set `TARGET` to a table you're allowed to create (format:
   `<catalog>.<schema>.jira_issue`). Set `JQL` to `project = KAN` (or your
   own project's key).
3. Run every cell top to bottom.

## Step 4 — Check these four things

1. **Row count.** `SELECT count(*) FROM <TARGET>` should equal the number of
   issues in that Jira project (check in the Jira UI).
2. **No duplicates.** Change `PAGE_SIZE` to something small (e.g. `3`) in the
   first cell, rerun, then check:
   `SELECT count(*), count(DISTINCT key) FROM <TARGET>` — the two numbers
   must be equal.
3. **Incremental works.** In the Jira UI, edit one issue (e.g. change its
   summary). Rerun the notebook. The read should be small (that one issue,
   plus a few from the overlap window) and the target's total row count
   should not change — only that one row's contents should update.
4. **No errors.** No exceptions, and nothing printed a credential or your
   Jira site's real hostname.

## Step 5 — Report back

Tell me:
- Did all four checks pass? If not, which one failed and what happened —
  paste the actual error text if there was one.
- The AIDP runtime info shown on the cluster (Spark version, Python version).
- How you supplied credentials (plain environment variables, or Vault).
- Anything that surprised you or needed a workaround.

I'll record the result in `connectors/jira/live-results/` (a dated PASS if
everything checked out, or NOT RUN with the reason if it didn't) and fold any
new gotchas into the docs — the same way the earlier live spike's
findings got written up.

## If something goes wrong

- **`ModuleNotFoundError: No module named 'aidp_http'` (or `aidp_secrets`):**
  the `_shared` folder isn't laid out at `HELPER_DIR/_shared` exactly as
  shown in Step 1 — check the file browser shows `_shared/` as a real
  subfolder there (not flattened, not nested one level deeper), and that
  `HELPER_DIR` is set correctly before the import cell runs.
- **Import error on `jira.py` itself:** check `HELPER_DIR` points at the
  folder containing `jira.py`, and that the notebook's first cell (the two
  `sys.path.insert(...)` lines) actually ran before the import cell.
- **Auth error (401/403):** double-check the API token was copied in full and
  the email matches the Atlassian account that owns it.
- **Anything else:** paste the exact error text (redact anything that looks
  like a hostname, email, or token first) and I'll help debug it live.
