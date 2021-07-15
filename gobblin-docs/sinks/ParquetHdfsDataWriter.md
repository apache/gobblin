# Description

An extension to [`FsDataWriter`](https://github.com/apache/gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/writer/FsDataWriter.java) that writes in Parquet format in the form of either Avro, Protobuf or [`ParquetGroup`](https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/example/data/Group.java). This implementation allows users to specify the CodecFactory to use through the configuration property [`writer.codec.type`](https://gobblin.readthedocs.io/en/latest/user-guide/Configuration-Properties-Glossary/#writercodectype). By default, the snappy codec is used. See [Developer Notes](#developer-notes) to make sure you are using the right Gobblin jar.

# Usage
```
writer.builder.class=org.apache.gobblin.writer.ParquetDataWriterBuilder
writer.destination.type=HDFS
writer.output.format=PARQUET
```

# Example Pipeline Configuration
* [`example-parquet.pull`](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/resources/example-parquet.pull) contains an example of generating test data and writing to Parquet files.


# Configuration

| Key                    | Description | Default Value | Required |
|------------------------|-------------|---------------|----------|
| writer.parquet.page.size | The page size threshold. | 1048576 | No |
| writer.parquet.dictionary.page.size | The block size threshold for the dictionary pages. | 134217728 | No |
| writer.parquet.dictionary | To turn dictionary encoding on. Parquet has a dictionary encoding for data with a small number of unique values ( < 10^5 ) that aids in significant compression and boosts processing speed. | true | No |
| writer.parquet.validate | To turn on validation using the schema. This validation is done by [`ParquetWriter`](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetWriter.java) not by Gobblin. | false | No |
| writer.parquet.version | Version of parquet writer to use. Available versions are v1 and v2. | v1 | No |
| writer.parquet.format | In-memory format of the record being written to Parquet. [`Options`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-parquet-common/src/main/java/org/apache/gobblin/parquet/writer/ParquetRecordFormat.java) are AVRO, PROTOBUF and GROUP | GROUP | No |

# Developer Notes

Gobblin provides integration with two different versions of Parquet through its modules. Use the appropriate jar based on the Parquet library you use in your code.

| Jar | Dependency | Gobblin Release |
|-----|-------------|--------|
| [`gobblin-parquet`](https://mvnrepository.com/artifact/org.apache.gobblin/gobblin-parquet) | [`com.twitter:parquet-hadoop-bundle`](https://mvnrepository.com/artifact/com.twitter/parquet-hadoop-bundle) | >= 0.12.0 |
| [`gobblin-parquet-apache`](https://mvnrepository.com/artifact/org.apache.gobblin/gobblin-parquet-apache) | [`org.apache.parquet:parquet-hadoop`](https://mvnrepository.com/artifact/org.apache.parquet/parquet-hadoop) | >= 0.15.0 |

If you want to look at the code, check out:

| Module | File |
| ------ | ---- |
| gobblin-parquet | [`ParquetHdfsDataWriter`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/writer/ParquetHdfsDataWriter.java) |
| gobblin-parquet | [`ParquetDataWriterBuilder`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-parquet/src/main/java/org/apache/gobblin/writer/ParquetDataWriterBuilder.java) |
| gobblin-parquet-apache | [`ParquetHdfsDataWriter`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-parquet-apache/src/main/java/org/apache/gobblin/writer/ParquetHdfsDataWriter.java) |
| gobblin-parquet-apache | [`ParquetDataWriterBuilder`](https://github.com/apache/gobblin/blob/master/gobblin-modules/gobblin-parquet-apache/src/main/java/org/apache/gobblin/writer/ParquetDataWriterBuilder.java) |
