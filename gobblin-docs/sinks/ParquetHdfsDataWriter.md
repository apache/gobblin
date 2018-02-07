# Description

An extension to [`FsDataWriter`](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/writer/FsDataWriter.java) that writes in Parquet format in the form of [`Group.java`](https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/example/data/Group.java). This implementation allows users to specify the CodecFactory to use through the configuration property [`writer.codec.type`](https://gobblin.readthedocs.io/en/latest/user-guide/Configuration-Properties-Glossary/#writercodectype). By default, the deflate codec is used.

# Usage
```
writer.builder.class=org.apache.gobblin.writer.ParquetDataWriterBuilder
writer.destination.type=HDFS
writer.output.format=PARQUET
```
For more info, see 
[`ParquetHdfsDataWriter`](https://github.com/apache/incubator-gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/writer/ParquetHdfsDataWriter.java)
and
[`ParquetDataWriterBuilder`](https://github.com/apache/incubator-gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/writer/ParquetDataWriterBuilder.java)


# Configuration

| Key                    | Description | Default Value | Required |
|------------------------|-------------|---------------|----------|
| writer.parquet.page.size | The page size threshold. | 1048576 | No |
| writer.parquet.dictionary.page.size | The block size threshold for the dictionary pages. | 134217728 | No |
| writer.parquet.dictionary | To turn dictionary encoding on. Parquet has a dictionary encoding for data with a small number of unique values ( < 10^5 ) that aids in significant compression and boosts processing speed. | true | No |
| writer.parquet.validate | To turn on validation using the schema. This validation is done by [`ParquetWriter`](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetWriter.java) not by Gobblin. | false | No |
| writer.parquet.version | Version of parquet writer to use. Available versions are v1 and v2. | v1 | No |