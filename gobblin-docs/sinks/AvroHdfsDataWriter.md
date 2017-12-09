# Description

Writes Avro records to Avro data files on Hadoop file systems.


# Usage


    writer.builder.class=org.apache.gobblin.writer.AvroDataWriterBuilder
    writer.destination.type=HDFS

For more info, see [`AvroHdfsDataWriter`](https://github.com/linkedin/gobblin/search?utf8=%E2%9C%93&q=AvroHdfsDataWriter)


# Configuration


| Key | Type | Description | Default Value |
|-----|------|-------------|---------------|
| writer.codec.type | One of null,deflate,snappy,bzip2,xz | Type of the compression codec | deflate |
| writer.deflate.level | 1-9 | The compression level for the "deflate" codec | 9 |

