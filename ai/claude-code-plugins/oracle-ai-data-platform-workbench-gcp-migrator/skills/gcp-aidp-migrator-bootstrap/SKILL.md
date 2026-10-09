---
name: gcp-aidp-migrator-bootstrap
description: "Readiness check for the Google Cloud to AIDP migrator. Verifies the gcp-aidp CLI, the Google service account key, aidp-cli, the ~/.oci/config profile and its key fingerprint, the AIDP instance OCID, workspace and cluster keys, the publish prefix and auth mode, and that the cluster is reachable and active. Use the first time the user runs the migrator on a machine, before the first publish, or when inventory, publish or run fails with an auth, config or connection error."
---

# `gcp-aidp-migrator-bootstrap`: readiness check

Checks everything `gcp-aidp` needs, one item at a time, and reports a table. It is
read-only: the one AIDP call lists the workspace's clusters. Re-run it after any
change of project, profile, instance, workspace or cluster.

**Never print a secret.** Do not read, print or copy the service account key, the
`.pem`, `~/.oci/config` values or `.env` values. Report shapes and results only
("an aidataplatform OCID, 89 characters"), as the commands below do.

Only check what the user's next step needs: steps 1–2 for `inventory`, all of them for
`publish` and `run`. Setup instructions for anything missing are in the README's
**Setup** section; point the user there rather than improvising.

## 1. The CLI

```bash
gcp-aidp --version || python3 -m gcp_aidp.cli --version
```

Missing → `pip install -e "${CLAUDE_PLUGIN_ROOT:-.}"`.

## 2. Google credentials (live inventory)

```bash
test -f "$GOOGLE_APPLICATION_CREDENTIALS" && echo "key file present" || echo "GOOGLE_APPLICATION_CREDENTIALS not set or missing"
python3 -c "
import google.auth, google.auth.transport.requests as r
c, p = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform.read-only'])
c.refresh(r.Request()); print('token ok, project', p)"
```

`ImportError` → `pip install -e '.[gcp]'`. A refresh error → the key is revoked or
wrong. The fixture (`--fixture demo`) needs none of this.

## 3. aidp-cli

```bash
command -v aidp && aidp version
```

Missing → `pip install aidp-cli`, ideally in its own venv with that venv's `bin` on
PATH: it conflicts with `oci-cli`'s pins.

## 4. The OCI profile

Profile = `$OCI_CLI_PROFILE`, else `DEFAULT`.

```bash
PROFILE="${OCI_CLI_PROFILE:-DEFAULT}"
grep -q "^\[$PROFILE\]" ~/.oci/config && echo "profile [$PROFILE] found" || echo "no [$PROFILE] header in ~/.oci/config"
awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f && /=/{split($0,a,"="); print a[1], length($0)}' ~/.oci/config
ls -l ~/.oci/config
```

Expect `user`, `fingerprint`, `tenancy`, `region` and `key_file` (or
`security_token_file`), and permissions `-rw-------`. Lengths are the tell for pasted
placeholders: a real OCID line is 80+ characters and `fingerprint=` is 59.

API key profiles: the fingerprint must match the key.

```bash
KEY=$(awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f && /^key_file=/{sub(/^key_file=/,""); print}' ~/.oci/config)
FP=$(openssl pkey -in "${KEY/#\~/$HOME}" -pubout -outform DER 2>/dev/null | openssl md5 -c | awk '{print $NF}')
awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f' ~/.oci/config | grep -qx "fingerprint=$FP" && echo "fingerprint matches key" || echo "fingerprint does NOT match key_file"
```

A mismatch means the config came from another API key. The key's own fingerprint is
safe to show; the user finds it under **Profile → API keys** in the OCI Console.

## 5. The `.env` values

Read from the shell, then the working directory's `.env`, then the plugin folder's.
Check the shape, never the value:

| Variable | Must be |
|---|---|
| `AIDP_INSTANCE_ID` | starts `ocid1.aidataplatform.` (a `ocid1.user.` or `ocid1.tenancy.` value is the most common mistake) |
| `AIDP_WORKSPACE_KEY`, `AIDP_CLUSTER_KEY` | UUIDs, 8-4-4-4-12 hex |
| `AIDP_PREFIX` | a letter, then letters, digits or underscores (no hyphens) |
| `AIDP_AUTH` | `api_key` when the profile has `key_file`; `security_token` (or unset) when it has `security_token_file` |

```bash
v(){ printenv "$1" || grep -hE "^$1=" .env "${CLAUDE_PLUGIN_ROOT:-.}/.env" 2>/dev/null | head -1 | cut -d= -f2- | sed -E 's/[[:space:]]+#.*//;s/^["'\'']//;s/["'\'']$//'; }
[[ $(v AIDP_INSTANCE_ID) == ocid1.aidataplatform.* ]] && echo "instance: ok" || echo "instance: not an aidataplatform OCID ($(v AIDP_INSTANCE_ID | cut -d. -f1-2))"
for k in AIDP_WORKSPACE_KEY AIDP_CLUSTER_KEY; do [[ $(v $k) =~ ^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$ ]] && echo "$k: ok" || echo "$k: not a UUID"; done
[[ $(v AIDP_PREFIX) =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] && echo "prefix: ok" || echo "prefix: invalid"
A=$(v AIDP_AUTH); WANT=api_key
awk -v p="[${OCI_CLI_PROFILE:-DEFAULT}]" '$0==p{f=1;next} /^\[/{f=0} f' ~/.oci/config | grep -q '^security_token_file=' && WANT=security_token
[ "${A:-security_token}" = "$WANT" ] && echo "auth: ok ($WANT)" || echo "auth: set AIDP_AUTH=$WANT (now: ${A:-unset, so aidp-cli uses security_token})"
```

## 6. AIDP reachability and the cluster

The same call `publish` makes. It proves the profile signs, the instance and workspace
are right, and the cluster is one the workspace can run jobs on:

```bash
aidp cluster list "<AIDP_WORKSPACE_KEY>" --instance-id "<AIDP_INSTANCE_ID>" \
    --profile "$PROFILE" --auth "<AIDP_AUTH>"
```

Substitute the values without echoing them. The output starts with a `Response:` line,
then JSON with `data.items[]`; find the item whose `key` is `AIDP_CLUSTER_KEY` and
report its `displayName` and `state`.

- `state` `ACTIVE` → ok. Anything else → start it in the workbench before `run`.
- Not listed → wrong cluster key, or the workspace's Default Master Catalog Compute,
  which cannot run notebook jobs. Show the listed clusters' names.
- `security_token auth requires security_token_file` → set `AIDP_AUTH=api_key`.
- 401 → the API key is not uploaded to that user, or the fingerprint is wrong (step 4).
- 404 or "not authorized" → wrong instance OCID or region, or the user lacks access
  to the instance or workspace (README **Setup**, step 2).

## 7. What the copy needs on the cluster

These live inside AIDP; ask the user to confirm them in the workbench rather than
probing:

- the cluster has `spark-bigquery-with-dependencies_2.12-<version>.jar` as a library;
- the credential store holds `gcp_bigquery_reader` with key `credentials_b64`;
- the target catalog exists.

`00_diagnose` checks all three on its first run.

## Output

```
| Check                  | Status | Notes |
|---|---|---|
| gcp-aidp CLI           | OK     | 0.1.0 |
| Google credentials     | OK     | token ok, project <id> |
| aidp-cli               | OK     | 4.2.1 |
| OCI profile            | OK     | [DEFAULT], fingerprint matches key |
| .env values            | OK     | instance, workspace, cluster, prefix, auth=api_key |
| AIDP cluster           | OK     | migrator, ACTIVE |
| On-cluster prereqs     | ASKED  | confirmed by the user / checked by 00_diagnose |
```

For every FAIL row give the exact fix. Do not go on to `publish` or `run` while a check
they depend on fails.
