# MongoDB Atlas Connector Sample

Load a MongoDB Atlas collection into a Delta table on Oracle AI Data Platform (AIDP) Workbench, using the MongoDB Spark Connector. The first run is a full load; later runs are incremental, using the newest value of a Date field already in the target table as the watermark. Read-only against MongoDB.

## Contents

| File | Purpose |
|---|---|
| `MongoDB_Atlas.ipynb` | Sample notebook: load the connector, check the connection, read the collection, then write or `MERGE` into Delta. |
| `mongodb_client.py` | Single helper module the notebook imports: URI validation, `mongodb+srv://` resolution on the driver, server-side watermark filter, schema widening, and error messages with the URI redacted. Standard library only. |
| `tests/` | Unit tests using fakes; no network, Spark or credentials needed. |

## Requirements

- A MongoDB Atlas cluster and a database user with read-only access to the collection.
- The connection string (`mongodb+srv://<user>:<password>@<cluster>.mongodb.net/`) in the AIDP [Credential Store](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/credential-store.html) as a **Secret Token** credential with key `uri`. The notebook reads it with `aidputils.secrets.get(name=..., key="uri")`. Outside AIDP, `credentials_from_env()` reads `MONGODB_URI` instead.
- The cluster's outbound IP on the Atlas project's IP access list.
- The MongoDB Spark Connector 10.7.0 and its driver 5.1.4 installed as cluster libraries, followed by a cluster restart. Download these five jars from Maven Central and check them against these SHA-256 values:

| Jar | SHA-256 |
|---|---|
| [mongo-spark-connector_2.12-10.7.0.jar](https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar) | `1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe` |
| [mongodb-driver-sync-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar) | `341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2` |
| [mongodb-driver-core-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar) | `ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec` |
| [bson-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson/5.1.4/bson-5.1.4.jar) | `bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676` |
| [bson-record-codec-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar) | `698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655` |

## Usage

1. Upload `mongodb_client.py` to a workspace folder.
2. Upload the five jars to a workspace folder, install each from the cluster's **Library** tab (**Install Library**, one at a time), and restart the cluster.
3. Create the Credential Store entry and add the cluster's outbound IP to the Atlas IP access list.
4. Open `MongoDB_Atlas.ipynb`, set `HELPER_DIR`, `CREDENTIAL_NAME`, `TARGET`, `DATABASE`, `COLLECTION` and `WATERMARK_FIELD`, and run the cells.

## Run the tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

## AIDP findings

Observed on 2026-10-01 on an AIDP cluster (Spark 3.5.0, Python 3.11.13):

- Notebooks cannot prompt for input: `getpass` raises `StdinNotImplementedError`. Use the Credential Store.
- `aidputils.secrets.get(name, key=None)` returns the whole credential as a map when `key` is omitted.
- Maven Central is reachable from the cluster, and the driver can write to `/Workspace/...` paths. Jars the driver wrote there did not appear in the **Install Library** file picker, though; jars uploaded through the workspace UI did.
- Loading the jars into a running session with `SparkContext.addJar` is not enough: the driver read from Atlas, but every executor task failed with `UnknownReason`, even `limit(1)`. Install the jars as cluster libraries instead.
- Installing a library while another library change is still running fails with "Unable to accept request due to an ongoing operation": install the jars one at a time.
- On the cluster tested, executors used the same outbound IP as the driver.
- With the jars installed as cluster libraries, the driver resolved `mongodb+srv://` DNS records but executors failed with `Failed looking up TXT record`. The notebook therefore calls `resolve_srv(spark, uri)`, which looks up the SRV and TXT records on the driver and passes executors an equivalent `mongodb://` URI. It is rebuilt on every run, so Atlas host changes are picked up.

## Live test

PASS on 2026-10-01: AIDP cluster with Spark 3.5.0 and Python 3.11.13, AMD workers (2 OCPUs, 32 GB) autoscaling from 1 to 10, an Atlas M0 cluster with the `sample_mflix` sample dataset, a read-only database user, and the cluster's outbound IP on the Atlas IP access list. The URI came from the Credential Store.

- First run: full load of `sample_mflix.comments`, 41,079 documents written to Delta.
- Second run from the top: 1 document read (the newest, inside the 300-second overlap) and merged; 41,079 rows and 41,079 distinct `_id` values.
- Not covered: jars reaching more than one executor (the failed tasks seen during the run were all on one executor), and a source with changing data.

## Known limits

- `resolve_srv` does not support the `srvMaxHosts` and `srvServiceName` URI options.

- A watermark does not see deletes in MongoDB.
- `WATERMARK_FIELD` must hold BSON Dates. A string field never matches a Date bound, so an incremental read returns nothing.
- A Spark task that fails with `UnknownReason` hides the real error. If even `df.limit(1).collect()` fails, the executors cannot use the connector.
- An IP missing from the Atlas access list does not look like a network error: it fails after the server-selection timeout with `SSLException: Received fatal alert: internal_error`. The helper names the likely cause and sets a 10 s timeout instead of the driver's 30 s.
- The connector infers the schema from a sample, and a value wider than the sampled type becomes `null` with no error. `read_collection` widens the inferred schema (decimals to `decimal(38, >=10)`, ints to longs) before reading.
- A field whose type varies between documents fails the read with `UnknownReason`; pass it in `STRING_FIELDS`, drop the target table and rerun.
- Later runs reuse the target table's schema, so fields that first appear after the first run are not added.
- `df.filter(col.isNull())` is pushed down to MongoDB and counts only server-side nulls; to check for lost values, count on the written table.
