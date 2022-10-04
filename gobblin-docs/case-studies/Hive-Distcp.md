Table of Contents
--------------------

[TOC]

# Introduction

Gobblin hive distcp is built on top of [Gobblin distcp](http://gobblin.readthedocs.io/en/latest/adaptors/Gobblin-Distcp/). It uses Hive metastore to find datasets to copy, then performs regular file listings to find the actual files to copy. After finishing the copy, the Hive registrations in the source are replicated on the target.

This document will show an sample job config of running Gobblin hive distcp, and explain how it works.

# Configure Hive Distcp Job

Below is the sample job config of running Gobblin hive distcp. Gobblin job constructs and data flow are the same as [Gobblin distcp](http://gobblin.readthedocs.io/en/latest/adaptors/Gobblin-Distcp/). The only difference is the `gobblin.data.profile.class` and hive related properties.

```
job.name=SampleHiveDistcp
job.group=HiveDistcp
job.description=Sample job config for hive distcp

extract.namespace=org.apache.gobblin.copy.tracking
gobblin.dataset.profile.class=org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder
data.publisher.type=org.apache.gobblin.data.management.copy.publisher.CopyDataPublisher
source.class=org.apache.gobblin.data.management.copy.CopySource
writer.builder.class=org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder
converter.classes=org.apache.gobblin.converter.IdentityConverter

hive.dataset.copy.target.table.prefixToBeReplaced=
hive.dataset.copy.target.table.prefixReplacement=
data.publisher.final.dir=${hive.dataset.copy.target.table.prefixReplacement}

hive.dataset.hive.metastore.uri=
hive.dataset.copy.target.metastore.uri=

hive.dataset.whitelist=
hive.dataset.copy.target.database=

hive.dataset.existing.entity.conflict.policy=REPLACE_PARTITIONS
hive.dataset.copy.deregister.fileDeleteMethod=NO_DELETE

hive.dataset.copy.location.listing.method=RECURSIVE
hive.dataset.copy.locations.listing.skipHiddenPaths=true

gobblin.copy.preserved.attributes=rgbp
```

## Source and target metastores

`hive.dataset.hive.metastore.uri` and `hive.dataset.copy.target.metastore.uri` specify the source and target metastore uri. Make sure the hive distcp job has access to both hive metastores.

## Database and tables to copy

Use a whitelist and optionally a blacklist using to specify tables to copy using the keys `hive.dataset.whitelist` and `hive.dataset.blacklist`. Both whitelist and blacklist accept various patterns, for example:

* sampleDb.sampleTable -> specific table `sampleTable` in database `sampleDb`;
* sampleDb -> all tables in database `sampleDb`;
* sampleDb.samplei* -> specific tables starting with `sample` in database `sampleDb`.

The key `hive.dataset.copy.target.database` specifies the target database to create tables under. If omitted, will use the same as the source.

## Target path computation

This specifies where copied files should be placed. There are a few options on how the target paths will be computed:

* Prefix replacement: simply replace a prefix in each file copied, e.g. /a/b to /a/btest. Use the keys `hive.dataset.copy.target.table.prefixToBeReplaced` and `hive.dataset.copy.target.table.prefixReplacement`. Any paths that are not a descendant of `prefixToBeReplaced` will throw an error and will fail the dataset. Note that setting both keys to "/" effectively replicates all paths exactly. 
* New table root: Puts files in a new table root. The source table root is its location (which is a Hive registration parameter). This mode will simply do a prefix replacement of the table root for each path in that table. Use the key `hive.dataset.copy.target.table.root` to specify the replacement. Note there is some primitive token replacement in the value of the key if using the tokens $DB and $TABLE, which will be replaced by the database and table name respectively. If the token $TABLE is not present, however, the table name will be automatically appended to the new table root (see last example below).
    * /data/$DB/$TABLE -> /data/databaseName/tableName
    * /data/$TABLE -> /data/tableName
    * /data -> /data/tableName
* Relocate files: This mode will move all files in a table to a structure matching Hive's native directory structure. I.e. all files for a partition "abc" of table "myDb.myTable" will be placed at path "<prefix>/abc" where prefix is specified using the key `hive.dataset.copy.target.table.root` and processed with the token replacements explained in "new table root". To enable this mode set `hive.dataset.copy.relocate.data.files` to true and set `hive.dataset.copy.target.table.root` appropriately. 
## Conflicting table and partitions treatment

If distcp-ng finds that a partition or table it needs to create already exists it will determine whether the existing table / partition is identical to what it would register (e.g. compare schema, location, etc.). If not, it will use a policy to determine how to proceed. The policy is specified using the key `hive.dataset.existing.entity.conflict.policy` and can take the following values:

* ABORT: the conflicting table will not be copied (default)
* REPLACE_PARTITIONS: replace any conflicting partitions, but not tables
* REPLACE_TABLES: replace any conflicting tables by deregistrating previous tables first.
* UPDATE_TABLES: Keep the original-registered table but make modification.

## Deregistering tables / partitions

Sometimes distcp-ng must deregister a table / partition, for example if it doesn't exist in the source, or if it must be replaced. In this case, distcp-ng offers options on what to do with the files under the deregistered partition. Set this policy using the key `hive.dataset.copy.deregister.fileDeleteMethod` which can take the following values:

* NO_DELETE: do not delete the files (default)
* INPUT_FORMAT: use the table / partition input format to infer which files are actually used by that table / partition, and delete only those files.
* RECURSIVE: delete the entire directory in the table / partition location.

## Finding copy files

To specify the files that distcp will copy for each table / partition, use the key `hive.dataset.copy.location.listing.method` which can take the values:

* INPUT_FORMAT: use the table / partition input format to infer which files are actually used by that table / partition. (default)
* RECURSIVE: copy all files under the directory in the table / partition location recursively.
If the recursive method is used, user can additionally specify `hive.dataset.copy.locations.listing.skipHiddenPaths`, which, if true, will not copy any hidden files.

## Partition Filter

A partition filter can be applied when copying partitioned tables. Filters can only be applied to text partition columns. To speficy a partition filter use the key `hive.dataset.copy.partition.filter.generator`.

* `gobblin.data.management.copy.hive.filter.LookbackPartitionFilterGenerator`: Filters date-representing partitions by a lookback (i.e. only copy recent partitions). Use the keys `hive.dataset.partition.filter.datetime.column`, `hive.dataset.partition.filter.datetime.lookback`, and `hive.dataset.partition.filter.datetime.format` to configure the filter.

## Fast partition skip predicate

A predicate that operates on partitions can be provided to distcp-ng to allow it to quickly skip partitions without having to list all of the source and target files and do a diff on those sets (a costly operation). To set this predicate, provide the class name of the predicate with the key `hive.dataset.copy.fast.partition.skip.predicate`. Below are the following predicates that exist

* `RegistrationTimeSkipPredicate`: This predicate compares the Hive partition attribute `registrationGenerationTimeMillis` in the target with the modification time of the partition directory in the source. The partition is skipped unless the directory was modified more recently than the registrationGenerationTime. The attribute `registrationGenerationTimeMillis` is an attribute set by distcp-ng representing (for all practical purposes) the time at which the distcp-ng job that registered that table started.
* `NonPartitionTableRegistrationTimeSkipPredicate`: This predicate can be used on non partition tables and compares the `registrationGenerationTimeMillis` in the target.
* `ExistingPartitionSkipPredicate`: This predicate can be used to skip any partition that already exists in the target table.
* `RootDirectoryModtimeSkipPredicate`: This predicate can be used to skip any partition whose root directory modified time is later than the copy source.
