[TOC]

# Introduction
--------------
The Gobblin Compliance module allows for data purging to meet regulatory compliance requirements. The module includes purging, retention and restore functionality for datasets.

The purging is performed using Hive meaning that purging of datasets is supported in any format that Hive can read from and write to, including for example ORC and Parquet. Further the purger is built on top of the Gobblin framework which means that the fault-tolerance, scalability and flexibility that Gobblin provides is taken full advantage of.

The [User Guide](../user-guide/Gobblin-Compliance) describes how to onboard a dataset for purging.

# Design
-------

The elements of the Compliance design are:

* The onboarding process
* The purge process
* The retention process
* The restore process

## Onboarding
-------------
A dataset is onboarded to the Purger with these steps:

1. The whitelist includes either of the database or table that will be considered for purging
2. Every table that is to be purged includes the necessary information for purging (dataset descriptor) as a JSON string in its TBLPROPERTIES

The purger iterates over all the tables that are whitelisted, and of those tables further looks for the presence of the dataset descriptor to specify the information required by the purger to proceed with the purge process.

With this information, the purger iterates over the partitions of the table that needs to be purged and proceeds to purge each partition of the table individually.

## Purger
---------
The purger code is mostly in the `gobblin.compliance.purger` package.

The elements of the purger are:

* The Gobblin constructs
* The Hive operations

### Gobblin constructs
----------------------
The Gobblin constructs that make up the Purger are:

* `HivePurgerSource` generates a WorkUnit per partition that needs to be purged
* `HivePurgerExtractor` instantiates a `PurgeableHivePartitionDataset` object that encapsulates all the information required to purge the partition
* For each partition, `HivePurgerConverter` populates the purge queries into the `PurgeableHivePartitionDataset` object
* The purge queries are executed by `HivePurgerWriter` 
* The `HivePurgerPublisher` moves successful Workunits to the `COMMITTED` state

### Hive operations
------------------
The purging process operates as follows:

* The partition information including location and partitioning scheme is determined from the metadata of the partition
* A new external staging table is created using the Hive `LIKE` construct of the current table that is being purged
* The location of this staging table on HDFS is a new folder within the table location with the current timestamp
* The purge query executes a `LEFT OUTER JOIN` of the original table against the table containing the ids whose data is to be purged and `INSERT OVERWRITE`s this data into the staging table, and thereby location. Once this query returns, the location will contain the purged data
* Since when we `ALTER` the original partition location next to the new staging table location, we preserve the location of the current/original location of the partition by creating a backup table pointing to this location. We do not move this immediately to avoid breaking any in-flight queries.
* The next step is to `ALTER` the partition location to the location containing the purged data
* The final step is to `DROP` the staging table, this only drops the metadata and not the data

Taking as an example, a `tracking.event` table, and the `datepartition=2017-02-16-00/is_guest=0` partition, the purge process would be the following:

* Let's assume the `tracking.event` table is located at the location `/user/tracking/event/`
* The full partition name would be `tracking@event@datepartition=2017-02-16-00/is_guest=0` per Hive, and let's assume the data is located at `/user/tracking/event/original/datepartition=2017-02-16-00/is_guest=0/`
* A staging table `tracking.event_staging_1234567890123` (`1234567890123` is the example timestamp we will use for clarity, a real timestamp looks more like '1487154972824') is created `LIKE tracking.event` with the location `/user/tracking/event/1234567890123/datepartition=2017-02-16-00/is_guest=0/`. This would be within the original table location
* The purge query would be similar to (assuming u_purger.guestids has the ids whose data is to be purged):
```hive
INSERT OVERWRITE TABLE tracking@event_staging_1234567890123
PARTITION (datepartition='2017-02-16-00',is_guest='0') 
SELECT /*+MAPJOIN(b) */ a.metadata.guestid, a.col_a, a.col_b 
FROM tracking.event a 
LEFT JOIN u_purger.guestids b
ON a.metadata.guestid=b.guestid
WHERE b.guestid IS NULL AND a.datepartition='2017-02-16-00' AND a.is_guest='0'
```
* A backup table `tracking.event_backup_1234567890123` is created with PARTITION `datepartition=2017-02-16-00,is_guest=0` pointing to the original location `/user/tracking/event/original/datepartition=2017-02-16-00/is_guest=0`
* The partition location of `tracking@event@2017-02-16-00` is updated to be `/user/tracking/event/1234567890123/datepartition=2017-02-16-00/is_guest=0`
* The `tracking.event_staging_1234567890123` table is dropped

## Retention
------------
The retention code is mostly in the `gobblin.compliance.retention` package.

The retention process builds on top of [Gobblin Retention](../data-management/Gobblin-Retention) and performs the following operations:

* Cleanup of backup data beyond a specified policy
* Cleanup of any staging tables not cleaned up in case of failures
* Reaping of backup locations from the original location
* Cleanup of trash data from the restore process beyond a specified policy

## Restore
----------
The restore code is mostly in the `gobblin.compliance.restore` package.

The restore process allows for restoration to a backup dataset if required.
