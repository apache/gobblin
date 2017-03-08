[TOC]

# Introduction
[`QueryBasedSource`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/QueryBasedSource.java)
represents a category of sources whose data is pulled by sending queries. A dataset of A source is identified as a
[`SourceEntity`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/QueryBasedSource.java#L96).
Query can be done by sending HTTP requests or SQL commands. A source often, not always, has a corresponding
[`QueryBasedExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/QueryBasedExtractor.java),
which defines the way and implements common routines to extract data from the source.

# Constructs
## `QueryBasedSource`
<p align="center">
  <figure>    
    <img src="/img/Gobblin-Query-Based-Sources.png" alt="Query based sources" width="800">
    <figcaption><br>Figure 1: Query based sources<br></figcaption>
  </figure>
</p>

Like other categories of sources, a `QueryBasedSource` focuses on creating work units as well. The way it does follows the general pattern:

- calculate low watermark of current run based on previous runs
- compute a high watermark
- partition datasets of current run into work units
- pick up peviously failed work units.

At last, it will group several work units as
[`MultiWorkUnit`](https://github.com/linkedin/gobblin/blob/master/gobblin-api/src/main/java/gobblin/source/workunit/MultiWorkUnit.java)
according to the `mr.job.max.mappers` configuration.

## `QueryBasedExtractor`
<p align="center">
  <figure>    
    <img src=/img/Gobblin-Query-Based-Extractors.png alt="Query based extractors" width="800">
    <figcaption><br>Figure 2: Query based extractors<br></figcaption>
  </figure>
</p>

Currently in Gobblin, depending on how an extractor communicates with a source
(or [different communiation protocols](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/ProtocolSpecificLayer.java)),
a `QueryBasedExtractor` falls into 2 categories:
[`RestApiExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/restapi/RestApiExtractor.java)
and
[`JdbcExtractor`](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/jdbc/JdbcExtractor.java).
A specific extractor has to provide some
[source specific logic](https://github.com/linkedin/gobblin/blob/master/gobblin-core/src/main/java/gobblin/source/extractor/extract/SourceSpecificLayer.java)
in order to successfully extract information from the source.

### `build`
Building a query based extractor may involve three queries:

<p align="center">
  <figure>    
    <img src=/img/Gobblin-Query-Based-Extractor-Build-Queries.png alt="Query based extractor build queries" width="400">
    <figcaption><br>Figure 3: Query based extractor build queries<br></figcaption>
  </figure>
</p>

- `extractMetadata` sends a query to fetch the data schema
- `getMaxWatermark` sends a query for calculating the latest high watermark
- `getSourceCount` sends a query for the total count of records to be pulled from the source

The actual implementations of those methods are pushed to an upper layer, which uses its own protocol(e.g. [Rest Api](../sources/RestApiSource.md) or Jdbc) to query the source.

### `readRecord`
While querying the record set for the last work unit, the upper bounds will be removed if appropriately. For a daily open-ended full dump job, it will fetch a more complete data set as there
might be some new data generated or existing data changes between the data query creation and execution. 

Two separate approaches to fetch record set:

- `getRecordSet`: A standard way to send a query, e.g. Rest api or Jdbc
- `getRecordSetFromSourceApi`: A specific way to send a query based on source api, e.g. Salesforce

Likewise, the actual implementations of those methods are pushed to an upper layer. See chapters: [Rest Api](../sources/RestApiSource.md), [Salesforce](../sources/SalesforceSource.md).


# Configuration
| Configuration Key | Default Value | Description |
| ----------------- | ------------- | ----------- |
| `source.querybased.schema` | Must Provide | Database name |
| `source.entity` | Must Provide | Name of the source entity that will be pulled from the source. It could be a database table, a source topic, a restful entity, etc. |
| `source.querybased.watermark.type` | Must Provide | The format of the watermark that is used when extracting data from the source. Possible types are timestamp, date, hour, simple. |
| `source.querybased.start.value` | Must provide | Value for the watermark to start pulling data from, also the default watermark if the previous watermark cannot be found in the old task states. |
| `source.querybased.end.value` | Optional | The high watermark the job should pull up to. |
| `extract.delta.fields` | Optional | List of columns that are associated with the watermark. |
| `source.querybased.skip.count.calc` | False | Disable calculating the total counts of records to be pulled from the source. |
| `source.querybased.is.specific.api.active` | False | True if this pull needs to use source specific apis instead of standard protocols. Ex: Use salesforce bulk api instead of rest api |





