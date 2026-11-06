# Stores

## Overview

Jobflow relies on the [maggma package](https://github.com/materialsproject/maggma) to provide a unified interface to a variety of data stores. By default, all calculations are run using a `MemoryStore`, which persists solely in the current process' memory. In production calculations, one will generally want to use a persistent data store, such as a MongoDB database. This also allows one to run calculations in a distributed manner with a common data store.

For a list of all available data stores, refer to the [maggma documentation](https://materialsproject.github.io/maggma/getting_started/stores/#list-of-stores). Here, we will go over how to use Jobflow with MongoDB via a [`MongoStore`](https://materialsproject.github.io/maggma/reference/stores/#maggma.stores.mongolike.MemoryStore).

## Configuring a `MongoStore`

### Creating a `jobflow.yaml` File

To modify basic Jobflow settings, you will first need to make a `jobflow.yaml` file if you haven't done so already. You will then need to define a `JOBFLOW_CONFIG_FILE` environment variable pointing to the file you made. For instance, in your `~/.bashrc` file, add the following line:

```bash
export JOBFLOW_CONFIG_FILE="/path/to/my/jobflow.yaml"
```

If this environment variable is not specified, Jobflow will look for the file in `~/.jobflow.yaml`.

### Basic Configuration

In your `jobflow.yaml` copy the example below and fill in the fields with the appropriate values for a MongoDB store.

```yaml title="jobflow.yaml"
JOB_STORE:
  docs_store:
    type: MongoStore
    host: <host name>
    port: 27017
    username: <username>
    password: <password>
    database: <database name>
    collection_name: <collection name>
```

### MongoDB Atlas

If you are using a URI (as is common with MongoDB Atlas), then you will instead have a `jobflow.yaml` file that looks like the example below. Here, you will put the full URI in the `host` field. The `username` and `password` are part of the URI and so should not be included elsewhere in the YAML file.

```yaml title="jobflow.yaml"
JOB_STORE:
  docs_store:
    type: MongoStore
    host: <URI>
    port: 27017
    database: <database name>
    collection_name: <collection name>
```

## MongoDB Indexes for JobStore Queries

`JobStore.connect()` does not create MongoDB indexes. Create these indexes as an operator for a
`MongoStore` JobStore when its queries need them. Do not use these commands for `MemoryStore`,
`JSONStore`, `FileStore`, or an S3 store with a separate index backend.

`OutputReference.resolve()` and `JobStore.get_output(..., "last")` filter by `uuid` and sort by
`index`. Create a non-unique compound index on the collection configured by
`docs_store.collection_name`:

```python
from pymongo import MongoClient

with MongoClient("<MongoDB URI>") as client:
    database = client["<database name>"]
    outputs = database["<docs_store collection_name>"]
    outputs.create_index([("uuid", 1), ("index", -1)])
```

This read index adds no uniqueness constraint to existing data. Its UUID prefix supports equality
lookups, and MongoDB can scan the index in either direction to select the first or last output.
See [sorting with indexes](https://www.mongodb.com/docs/manual/tutorial/sort-results-with-indexes/).

If an additional blob store is configured, index its collection for the blob lookup by
`blob_uuid`. For a `MongoStore`, use its configured `collection_name`:

```python
with MongoClient("<MongoDB URI>") as client:
    database = client["<database name>"]
    database["<additional MongoStore collection_name>"].create_index("blob_uuid")
```

For a `GridFSStore`, `collection_name` is the bucket name. Its files collection uses the field
`metadata.blob_uuid`:

```python
with MongoClient("<MongoDB URI>") as client:
    database = client["<database name>"]
    database["<GridFSStore collection_name>.files"].create_index("metadata.blob_uuid")
```

Inspect existing indexes with `collection.index_information()` before creating one. Check the key
fields and their order. Index creation requires an account with index-management privileges. Indexes
use storage, and MongoDB can report errors for conflicting definitions or insufficient privileges.
`create_index()` raises those errors to the caller. Jobflow does not create these indexes or hide
creation failures. See [createIndex](https://www.mongodb.com/docs/manual/reference/method/db.collection.createIndex/).

Check the planner with an unhinted query that matches the latest-output lookup. Use a representative
UUID and data set. This projection matches the `JobStore` output query. PyMongo includes `_id`
unless the query excludes it.

```python
with MongoClient("<MongoDB URI>") as client:
    outputs = client["<database name>"]["<docs_store collection_name>"]
    plan = outputs.database.command(
        "explain",
        {
            "find": outputs.name,
            "filter": {"uuid": "<job UUID>"},
            "projection": {"output": 1, "uuid": 1, "index": 1},
            "sort": {"index": -1},
            "limit": 1,
        },
        verbosity="executionStats",
    )
```

Inspect `queryPlanner.winningPlan` for the selected index and any blocking sort. Record
`executionStats.totalKeysExamined`, `totalDocsExamined`, and `nReturned`. The output projection
includes `_id`, and returning the output requires document data. This is not a covered query. An
index definition alone does not establish planner selection or a speedup. See
[explain](https://www.mongodb.com/docs/manual/reference/method/cursor.explain/).

## Additional Details

For additional details on how to specify a data store as well as the various settings available to modify in Jobflow, refer to the [API documentation](https://materialsproject.github.io/jobflow/jobflow.settings.html) for `jobflow.settings`.
