# Table of Contents

[TOC]

Gobblin has the ability to register the ingested/compacted data in Hive. This allows registering data in Hive immediately after data is published at the destination, offering much lower latency compared to doing data ingestion and Hive registration separately.

## How Hive Registration Works in Gobblin

Hive registration is done in [`HiveRegister`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/HiveRegister.java). After the data is published, the publisher or compaction runner will create an instance of `HiveRegister`, and for each path that should be registered in Hive, the publisher or compaction runner will use a specific [`HiveRegistrationPolicy`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/policy/HiveRegistrationPolicy.java) to create a list of [`HiveSpec`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/spec/HiveSpec.java)s for the path. It creates a list of `HiveSpec`s rather than a single `HiveSpec` for each path, so that the same path can be registered in multiple tables or databases.

### `HiveSpec`

A `HiveSpec` specifies how a path should be registered in Hive, i.e., which database, which table, which partition should the path be registered. An example is [`SimpleHiveSpec`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/spec/SimpleHiveSpec.java).

### `HiveRegistrationPolicy`

`HiveRegistrationPolicy` is responsible for generating `HiveSpec`s given a path. For example, if you want paths ending with a date (e.g., `/(something)/2016/05/22`) to be registered in the corresponding daily partition (e.g., `daily-2016-05-22`), you can create an implementation of `HiveRegistrationPolicy` that contains the logic of converting such a path into a Hive partition. 

An example is [`HiveRegistrationPolicyBase`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/policy/HiveRegistrationPolicyBase.java), which provides base implementation for getting database names and table names for a path:

* A database/table name can be specified explicitly in `hive.database.name` or `hive.table.name`.
* Alternatively, a database/table regex can be provided in `hive.database.regex` or `hive.table.regex`. The regex will be matched against the path to be registered, and if they match, the first group is considered the database/table name.
* It is possible to register a path to multiple databases or tables by specifying `additional.hive.database.names` and `additional.hive.table.names`. If multiple databases and tables are specified, the path will be registered to the cross product.
* If the provided/derived Hive database/table names are invalid, they are sanitized into a valid name. A database/table name is valid if it starts with an alphanumeric character, contains only alphanumeric characters and `_`, and is not composed of numbers only.

One should in general extend `HiveRegistrationPolicyBase` when implementing a new `HiveRegistrationPolicy`.

### `HiveSerDeManager`

If the data to be registered is in a format other than plain text (CSV, TSV, etc.), you often need to use a SerDe and specify some SerDe properties including the type of SerDe, input format, output format, schema, etc. This is done in [`HiveSerDeManager`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/HiveSerDeManager.java), which is part of a [`HiveRegistrationUnit`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/HiveRegistrationUnit.java) (i.e., [`HiveTable`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/HiveTable.java) or [`HivePartition`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/HivePartition.java)). An example is [`HiveAvroSerDeManager`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/avro/HiveAvroSerDeManager.java).

### Predicate and Activity

One or more `Predicate`s can be attached to a `HiveSpec`. If a `HiveSpec` contains `Predicate`s, unless `Predicate`s return `true`, the `HiveSpec` will not be registered. This is useful in cases where, for example, one only wants to register a path in Hive if a particular Hive table or partition doesn't already exist. An example is [`TableNotExistPredicate`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/spec/predicate/TableNotExistPredicate.java).

One or more [`Activity`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/spec/activity/Activity.java)s can be attached to a `HiveSpec`. There are two types of activities: pre-activities and post-activities, which will be executed before and after a `HiveSpec` is registered, respectively. This is useful, for example, when you need to drop/alter a table/partition before or after a path is registered. An example is [`DropTableActivity`](https://github.com/linkedin/gobblin/blob/master/gobblin-hive-registration/src/main/java/gobblin/hive/spec/activity/DropTableActivity.java).

## How to Use Hive Registration in Your Gobblin Job

First, is to implement a `HiveRegistrationPolicy` (or reuse an existing one), then specify its class name in config property `hive.registration.policy`.

Then, specify the appropriate table/partition properties in `hive.table.partition.props`, storage descriptor properties in 
`hive.storage.props`, and SerDe properties in `hive.serde.props`. Some SerDe properties are usually dynamic (e.g., schema), which are added in the `HiveSerDeManager`.

Example table/partition properties are "owner" and "retention", example storage descriptor properties are "location", "compressed", "numBuckets", example SerDe properties are "serializationLib", "avro.schema.url".

If you are running a Gobblin ingestion job:

* If data is published in the job (which is the default case), use a job-level data publisher that can perform Hive registration, such as [`BaseDataPublisherWithHiveRegistration`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/publisher/BaseDataPublisherWithHiveRegistration.java). If you need to do Hive registration with a different publisher than `BaseDataPublisher`, you will need to extend that publisher to do Hive registration, which will be similar as how `BaseDataPublisher` is extended into `BaseDataPublisherWithHiveRegistration`.
* If data is published in the tasks, use [`HiveRegistrationPublisher`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/publisher/HiveRegistrationPublisher.java) as the job-level data publisher. This publisher does not publish any data; it only does Hive registration.

If you are running a Gobblin compaction job: add [`HiveRegistrationCompactorListener`](https://github.com/linkedin/gobblin/blob/master/gobblin-compaction/src/main/java/gobblin/compaction/hive/registration/HiveRegistrationCompactorListener.java) to the list of compaction listeners by adding the class name to property `compaction.listeners`.

## Hive Registration Config Properties

| Property Name  | Semantics  |
|---|---|
| `hive.registration.policy` | Class name which implements `HiveRegistrationPolicy` |
| `hive.row.format` | Either `AVRO`, or the class name which implements `HiveSerDeManager`|
| `hive.database.name` | Hive database name |
| `hive.database.regex` | Hive database regex |
| `hive.database.name.prefix` | Hive database name prefix |
| `hive.database.name.suffix` | Hive database name suffix |
| `additional.hive.database.names` | Additional Hive database names |
| `hive.table.name` | Hive table name |
| `hive.table.regex` | Hive table regex |
| `hive.table.name.prefix` | Hive table name prefix |
| `hive.table.name.suffix` | Hive table name suffix |
| `additional.hive.table.names` | Additional Hive table names |
| `hive.register.threads` | Thread pool size used for Hive registration |
| `hive.db.root.dir` | The root dir of Hive db |
| `hive.table.partition.props` | Table/partition properties |
| `hive.storage.props` | Storage descriptor properties |
| `hive.serde.props` | SerDe properties |
| `hive.registration.fs.uri` | File system URI for Hive registration |
| `hive.upstream.data.attr.names` | Attributes to describe upstream data source as Hive Metadata |


