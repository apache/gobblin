[TOC]

# Introduction
--------------
The Gobblin Compliance module allows for data purging to meet regulatory compliance requirements. The module includes functionality for purging datasets and the associated operational support in production.

The purging is performed using Hive meaning that purging of datasets is supported in any format that Hive can read from and write to, including for example ORC and Parquet. Further the purger is built on top of the Gobblin framework which means that the fault-tolerance, scalability and flexibility provided by Gobblin is taken full advantage of.

# Usage
-------
As an example, let us assume that regulation requires that once a guest has checked out of a hotel, certain guest data needs to be purged within a certain number of days after the guest leaves. Hence, the goal is to purge the hotel's datasets of data associated with their guests after they have left in order to meet regulatory compliance requirements.

Hive databases and tables are setup to be purged by following these steps:

1. Whitelisting of the database, or table for purging
2. Specifying the dataset descriptor for the tables to be purged
3. Specifying the JSON path of the compliance field in the dataset descriptor
4. The table which contains the list of ids whose associated data is to be purged
5. The name of the id column in the table to match against

For example, in order to purge a Hive table named `tracking.event`, these properties are specified:

* Specify the purger whitelist to include tracking.event
`gobblin.compliance.dataset.whitelist=tracking.event`
* Add a TBLPROPERTY named `dataset.descriptor` to the tracking.event Hive table to specify the compliance field to match in the table as an escaped JSON (since it has to be a valid string):
`{\"complianceSpec\" : {\"identifierField\" : \"metadata.guestid\" }}`
* Specify the JSON field path for the compliance field (that evaluates to metadata.guestId)
`dataset.descriptor.fieldPath=complianceSpec.identifierField`
* Specify the table containing the list of ids to purge
`gobblin.compliance.complianceIdTable=u_purger.guestIds`
* Specify the name of the id in the table to match against
`gobblin.compliance.complianceId=guestId`

With these properties in place, a Gobblin job can be setup to purge the table. The work unit for the purger is an individual table partition. Hence the purger will iterate over all the partitions in the table, and purge each partition individually, processing as many partitions in parallel as specified (by the property 'gobblin.compliance.purger.maxWorkunits', which defaults to 5).

# Configuration
---------------
Configuration options for the Hive Purger include

| Property      | Description |
| ------------- |-------------|
| gobblin.compliance.dataset.whitelist | The list of databases/tables to purge, comma-separated |
| dataset.descriptor.fieldPath | The JSON field path specifying the compliance field |
| gobblin.compliance.complianceIdTable | The table containing the list of ids whose data needs to be purged |
| gobblin.compliance.complianceId | The name of the id column in the complianceIdTable to match against |
| gobblin.compliance.purger.maxWorkunits | The number of partitions to purge in parallel |
| gobblin.compliance.purger.policy.class | The policy class that specifies the criteria for purging, defaults to HivePurgerPolicy |
| gobblin.compliance.purger.commit.policy.class | The policy class that specifies the criteria for committing a purged dataset, defaults to HivePurgerCommitPolicy |

# Developer Guide

The [Developer Guide](../developer-guide/Gobblin-Compliance-Design) further describes the design of the module.

