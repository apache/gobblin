Currently, Gobblin supports the following feature list:

# Different Data Sources

|Source Type|Protocol|Vendors|
|-----------|--------|-------|
|RDMS|JDBC|MySQL/SQLServer|
|Files|HDFS/SFTP/LocalFS|N/A|
|Salesforce|REST|Salesforce|

* Different Pulling Types
    * SNAPSHOT-ONLY: Pull the snapshot of one dataset.
    * SNAPSHOT-APPEND: Pull delta changes since last run, optionally merge delta changes into snapshot (Delta changes include updates to the dataset since last run).
    * APPEND-ONLY: Pull delta changes since last run, and append to dataset.

* Different Deployment Types
    * standalone deploy on a single machine
    * cluster deploy on hadoop 2.3.0

* Compaction
    * Merge delta changes into snapshot.
