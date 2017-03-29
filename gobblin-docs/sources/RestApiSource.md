[TOC]

# Introduction
A RestApiSource is a [QueryBasedSource](../sources/QueryBasedSource.md) which uses [RESTful](https://en.wikipedia.org/wiki/Representational_state_transfer)
Api for query. `RestApiExtractor` is a `QueryBasedExtractor` that uses REST to communicate with the source. To establish the communication,
a [`RestApiConnector`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/restapi/RestApiConnector.java) is
required.

# Constructs
## `RestApiSource`
Coming soon...

## `RestApiExtractor`
A `RestApiExtractor` sets up the common routines to query information from a REST source, for example, `extractMetadata`,
`getMaxWatermark`, `getSourceCount`, `getRecordSet`, which are mentioned in chapter [QueryBasedSource](../sources/QueryBasedSource.md).
In terms of constructing the actual query and extracting the data from the response, the source specific layer holds the truth,
for example, `SalesforceExtractor`.

A simplified general flow of routines is depicted in Figure 1:

<p align="center">
  <figure>    
    <img src=../../img/Rest-Api-Extractor-Flow.png alt="Rest api extractor general routine flow" width="500">
    <figcaption><br>Figure 1: RestApiExtractor general routine flow <br></figcaption>
  </figure>
</p>

Depends on the routines, [getX], [constructGetXQuery], [extractXFromResponse] are

| Description | [getX] | [constructGetXQuery] | [extractXFromResponse] |
| ----------- | ------ | -------------------- | ---------------------- |
| Get data schema | `extractMetadata` | `getSchemaMetadata` | `getSchema` |
| Calculate latest high watermark | `getMaxWatermark` | `getHighWatermarkMetadata` | `getHighWatermark` |
| Get total counts of records to be pulled | `getSourceCount` | `getCountMetadata` | `getCount` |
| Get records | `getRecordSet` | `getDataMetadata` | `getData` |

There are other interactions between the `RestApiExtractor` layer and `SourceSpecificLayer`. The key points are:

- A [`ProtocolSpecificLayer`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/ProtocolSpecificLayer.java), such as
`RestApiExtractor`, understands the protocol and sets up a routine to communicate with the source
- A `SourceSpecificLayer`, such as `SalesforceExtractor`, knows the source and fits into the routine by providing and analyzing source specific information

# Configuration
| Configuration Key | Default Value | Description |
| ----------------- | ------------- | ----------- |
| `source.querybased.query` | Optional | The query that the extractor should execute to pull data. |
| `source.querybased.excluded.columns` | Options | Names of columns excluded while pulling data. |
| `extract.delta.fields` | Optional | List of columns that are associated with the watermark. |
| `extract.primary.key.fields` | Optional | List of columns that will be used as the primary key for the data. |


