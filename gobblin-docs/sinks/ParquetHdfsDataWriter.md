# Description

Writes Parquet records to Parquet data files on Hadoop file systems.


# Usage

    writer.builder.class=org.apache.gobblin.writer.ParquetHdfsDataWriter
    writer.destination.type=HDFS

For more info, see [`ParquetHdfsDataWriter`](https://github.com/apache/incubator-gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/writer/ParquetHdfsDataWriter.java)


# Configuration

| Key                    | Description | Default Value | Required |
|------------------------|-------------|---------------|----------|
| writer.parquet.page.size | The page size threshold | 1048576 | No |
| writer.parquet.dictionary.page.size | The block size threshold. | 134217728 | No |
| writer.parquet.dictionary | To turn dictionary encoding on. | true | No |
| writer.parquet.validate | To turn on validation using the schema. | false | No |
| writer.parquet.version | Version of parquet writer to use. Available versions are v1 and v2. | v1 | No |

