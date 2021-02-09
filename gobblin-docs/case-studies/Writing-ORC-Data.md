# Table of Contents

[TOC]

# Introduction

Gobblin is capable of writing data to ORC files by leveraging Hive's SerDe library. Gobblin has native integration with Hive SerDe's library via the [HiveSerDeWrapper](https://github.com/apache/gobblin/blob/master/gobblin-hive-registration/src/main/java/org/apache/gobblin/hive/HiveSerDeManager.java) class.

This document will briefly explain how Gobblin integrates with Hive's SerDe library, and show an example of writing ORC files.

# Hive SerDe Integration

[Hive's SerDe library](https://cwiki.apache.org/confluence/display/Hive/SerDe) defines the interface Hive uses for serialization and deserialization of data. The Hive SerDe library has out of the box SerDe support for Avro, ORC, Parquet, CSV, and JSON SerDes. However, users are free to define custom SerDes.

Gobblin integrates with the Hive SerDe's in a few different places. Here is a list of integration points that are relevant for this document:

* `HiveSerDeWrapper` wrapper around Hive's SerDe library that provides some nice utilities and structure that the rest of Gobblin can interfact with
* `HiveSerDeConverter` takes a `Writable` object in a specific format, and converts it to the Writable of another format (e.g. from `AvroGenericRecordWritable` to `OrcSerdeRow`)
* `HiveWritableHdfsDataWriter` writes a `Writable` object to a specific file, typically this writes the output of a `HiveSerDeConverter`

# Writing to an ORC File

An end-to-end example of writing to an ORC file is provided in the configuration found [here](https://github.com/apache/gobblin/blob/master/gobblin-example/src/main/resources/wikipedia-orc.pull). This `.pull` file is almost identical to the Wikipedia example discussed in the [Getting Started Guide](../Getting-Started.md). The only difference is that the output is written in ORC instead of Avro. The configuration file mentioned above can be directly used as a template for writing data to ORC files, below is a detailed explanation of the configuration options that need to be changed, and why they need to be changed.

* `converter.classes` requires two additional converters: `gobblin.converter.avro.AvroRecordToAvroWritableConverter` and `gobblin.converter.serde.HiveSerDeConverter`
    * The output of the first converter (the `WikipediaConverter`) returns Avro `GenericRecord`s
    * These records must be converted to `Writable` object in order for the Hive SerDe to process them, which is where the `AvroRecordToAvroWritableConverter` comes in
    * The `HiveSerDeConverter` does the actual heavy lifting of converting the Avro Records to ORC Records
* In order to configure the `HiveSerDeConverter` the following properites need to be added:
    * `serde.deserializer.type=AVRO` says that the records being fed into the converter are Avro records
        * `avro.schema.literal` or `avro.schema.url` must be set when using this deserializer so that the Hive SerDe knows what Avro Schema to use when converting the record
    * `serde.serializer.type=ORC` says that the records that should be returned by the converter are ORC records
* `writer.builder.class` should be set to `gobblin.writer.HiveWritableHdfsDataWriterBuilder`
    * This writer class will take the output of the `HiveSerDeConverter` and write the actual ORC records to an ORC file
* `writer.output.format` should be set to `ORC`; this ensures the files produced end with the `.orc` file extension
* `fork.record.queue.capacity` should be set to `1`
    * This ensures no caching of records is done before they get passed to the writer; this is necessary because the `OrcSerde` caches the object it uses to serialize records, and it does not allow copying of Orc Records

The example job can be run the same way the regular Wikipedia job is run, except the output will be in the ORC format.

## Data Flow

For the Wikipedia to ORC example, data flows in the following manner:

* It is extracted from Wikipedia via the `WikipediaExtractor`, which also converts each Wikipedia entry into a `JsonElement`
* The `WikipediaConverter` then converts the Wikipedia JSON entry into an Avro `GenericRecord`
* The `AvroRecordToAvroWritableConverter` converts the Avro `GenericRecord` to a `AvroGenericRecordWritable`
* The `HiveSerDeConverter` converts the `AvroGenericRecordWritable` to a `OrcSerdeRow`
* The `HiveWritableHdfsDataWriter` uses the `OrcOutputFormat` to write the `OrcSerdeRow` to an `OrcFile`

# Extending Gobblin's SerDe Integration

While this tutorial only discusses Avro to ORC conversion, it should be relatively straightfoward to use the approach mentioned in this document to convert CSV, JSON, etc. data into ORC.
