# MongoDB Atlas Connector Sample

Load a MongoDB Atlas collection into a Delta table on Oracle AI Data Platform (AIDP) Workbench, using the MongoDB Spark Connector. The first run is a full load; later runs are incremental, using the newest value of a Date field already in the target table as the watermark. Read-only against MongoDB.

## Contents

| File | Purpose |
|---|---|
| `MongoDB_Atlas.ipynb` | Sample notebook: load the connector, check the connection, read the collection, then write or `MERGE` into Delta. |
| `mongodb_client.py` | Single helper module the notebook imports: credential lookup (OCI Vault, then environment), SHA-256-pinned runtime jar loading, server-side watermark filter, schema widening, and error messages with the URI redacted. Needs no PyPI package (plus `oci` if you use OCI Vault). |
| `tests/` | Unit tests using fakes; no network, Spark or credentials needed. |

## Requirements

- A MongoDB Atlas cluster and a database user with read-only access to the collection.
- `MONGODB_URI` (`mongodb+srv://<user>:<password>@<cluster>.mongodb.net/`) as a cluster environment variable, or as an OCI Vault secret with `OCI_VAULT_ID` set.
- The cluster's outbound IP on the Atlas project's IP access list.
- Network access from the cluster to Maven Central (`repo1.maven.org`). The MongoDB Spark Connector 10.7.0 and driver 5.1.4 jars are downloaded to `/tmp` at runtime, checked against pinned SHA-256 values, and loaded into the running session.
- Only for OCI Vault credentials: the `oci` package and an OCI config the cluster can read (`~/.oci/config`). Secrets are looked up in `OCI_COMPARTMENT_ID` if set, otherwise the tenancy root. If the Vault lookup fails, the connector falls back to environment variables and reports the Vault error if those are missing too.

## Usage

1. Upload `mongodb_client.py` to a workspace folder.
2. Open `MongoDB_Atlas.ipynb`, set `HELPER_DIR`, `TARGET`, `DATABASE`, `COLLECTION` and `WATERMARK_FIELD`, and run the cells.

## Run the tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Known limits

- A watermark does not see deletes in MongoDB.
- `WATERMARK_FIELD` must hold BSON Dates. A string field never matches a Date bound, so an incremental read returns nothing.
- An IP missing from the Atlas access list does not look like a network error: it fails after the server-selection timeout with `SSLException: Received fatal alert: internal_error`. The helper names the likely cause and sets a 10 s timeout instead of the driver's 30 s.
- The connector infers the schema from a sample, and a value wider than the sampled type becomes `null` with no error. `read_collection` widens the inferred schema (decimals to `decimal(38, >=10)`, ints to longs) before reading.
- A field whose type varies between documents fails the read with `UnknownReason`; pass it in `STRING_FIELDS`, drop the target table and rerun.
- Later runs reuse the target table's schema, so fields that first appear after the first run are not added.
- `df.filter(col.isNull())` is pushed down to MongoDB and counts only server-side nulls; to check for lost values, count on the written table.
