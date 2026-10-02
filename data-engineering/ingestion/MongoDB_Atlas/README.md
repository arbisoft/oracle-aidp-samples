# MongoDB Atlas Connector Samples

`MongoDB_Atlas.ipynb` reads a MongoDB Atlas collection into Spark on AIDP and loads it into a Delta table: a full load on the first run, then incremental loads on a Date field, merged on `_id`. Read-only against MongoDB.

The logic lives in the [`aidp-connector-mongodb`](https://github.com/arbisoft/oracle-aidp-connectors/tree/main/src/aidp_connector_mongodb) package, unit-tested there; the notebook configures and calls it. That package's README covers every option and the known limits.

## Cluster libraries

Two files, both built from that package's folder and installed as cluster libraries, then a cluster restart:

| File | Built with | Holds |
|---|---|---|
| `mongo-spark-connector-bundle_2.12-10.7.0.jar` | `python build_connector_jar.py` | The MongoDB Spark Connector 10.7.0 and its four driver jars, unmodified, from Maven Central, each checked against its SHA-256. |
| `aidp_connector_mongodb-<version>-py3-none-any.whl` | `uv build` | The connector package. |

## AIDP notes

Observed on AIDP clusters on 2026-10-01 and 2026-10-02:

- The jars must be cluster libraries. Loading them at runtime with `SparkContext.addJar` is not enough: the driver reads, but every executor task fails with `UnknownReason`. Spark cannot fetch them itself either: the cluster Spark property `spark.jars.packages` is rejected as reserved.
- The combined jar works on its own, with no other MongoDB jars installed, including a read spread over two executors.
- Only one library change runs at a time per cluster: installing while another install is still running fails with "ongoing operation".
- Executors cannot resolve `mongodb+srv://` (`Failed looking up TXT record`); the package resolves it on the driver.
- An outbound IP missing from the Atlas access list fails after the server-selection timeout with `SSLException: Received fatal alert: internal_error`. Check the IP again after a cluster restart.
- Notebooks cannot prompt for input (`getpass` fails); use the Credential Store.

## Loading several collections

One configuration loads one collection into one table. To load several, run the notebook once per collection with its own configuration, or schedule one job per collection.

## Limits

- An incremental run does not see deletes in MongoDB. A full refresh, `run(spark, config, uri, "full")`, reloads the collection and overwrites the table.
- `watermark_field` must hold BSON Dates the application updates on every write. Without one, every run re-reads the whole collection.
- Later runs reuse the table's schema, so fields that first appear later are added only by a full refresh.
