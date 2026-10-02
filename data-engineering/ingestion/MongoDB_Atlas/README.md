# MongoDB Atlas Connector Samples

`MongoDB_Atlas.ipynb` reads a MongoDB Atlas collection into Spark on AIDP with the MongoDB Spark Connector, and loads it into a Delta table: a full load on the first run, then incremental loads on a Date field, merged on `_id`. Read-only against MongoDB.

## Connector jars

Install these five jars as cluster libraries, then restart the cluster. The notebook's *One-Time Setup* cell downloads them into a workspace folder and checks each SHA-256; or download them yourself from the links below. Install each from the cluster's **Library** tab → **Install Library**, one at a time.

| Jar | SHA-256 |
|---|---|
| [mongo-spark-connector_2.12-10.7.0.jar](https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar) | `1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe` |
| [mongodb-driver-sync-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar) | `341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2` |
| [mongodb-driver-core-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar) | `ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec` |
| [bson-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson/5.1.4/bson-5.1.4.jar) | `bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676` |
| [bson-record-codec-5.1.4.jar](https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar) | `698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655` |

## AIDP notes

Observed on 2026-10-01 on an AIDP cluster (Spark 3.5.0, Python 3.11.13):

- This notebook ran top to bottom against Atlas's `sample_mflix.comments`: the first run loaded 41,079 documents into a new Delta table, and a second run of the incremental load merged into it with no duplicates, 41,079 rows and 41,079 distinct `_id` values.
- Loading the jars at runtime with `SparkContext.addJar` is not enough: the driver reads, but every executor task fails with `UnknownReason`. Install them as cluster libraries.
- Only one library change runs at a time per cluster: installing a jar while another install is still running fails with "ongoing operation".
- Executors cannot resolve `mongodb+srv://` (`Failed looking up TXT record`); the notebook resolves it on the driver.
- An outbound IP missing from the Atlas access list fails after the server-selection timeout with `SSLException: Received fatal alert: internal_error`. Check the IP again after a cluster restart.
- If even a one-row read fails with `UnknownReason`, the executors are missing the jars: check they are installed as cluster libraries and the cluster was restarted.
- Notebooks cannot prompt for input (`getpass` fails); use the Credential Store.

## Limits

- A watermark does not see deletes in MongoDB.
- Later runs only pick up documents whose `WATERMARK_FIELD` moves forward, so it should be a last-modified time the application sets on every write. A creation time does not catch edits.
- `WATERMARK_FIELD` must hold BSON Dates. The first run loads every document, but a document whose field is missing, null or not a Date is never re-read by later runs.
- Later runs reuse the target table's schema, so fields that first appear later are not added.
- `df.filter(col.isNull())` is pushed down to MongoDB and counts only server-side nulls; count on the written table instead.
- The SRV resolution does not support the `srvMaxHosts` and `srvServiceName` URI options.
