---
title: HDFS Bytes Array
sidebar_label: HDFS Bytes Array
---

# Description


A simple writer for byte arrays to a Hadoop file system file. The byte arrays can be optionally prefixed by a long-sized length and/or record delimiter byte.

# Usage

```properties
    writer.builder.class=org.apache.gobblin.writer.AvroDataWriterBuilder
```
# Configuration


| Key | Type | Description | Default Value |
|-----|------|-------------|---------------|
| simple.writer.delimiter | character | An optional character to be used as records separator |  |
| simple.writer.prepend.size | boolean | Enables/disables pre-pending the bytes written with a long size | false |

