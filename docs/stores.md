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

## Additional Details

For additional details on how to specify a data store as well as the various settings available to modify in Jobflow, refer to the [API documentation](https://materialsproject.github.io/jobflow/jobflow.settings.html) for `jobflow.settings`.
