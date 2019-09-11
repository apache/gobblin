Table of Contents
-----------------

[TOC]

Gobblin ETL comes equipped with instrumentation using [Gobblin Metrics](Gobblin Metrics), as well as end points to easily extend this instrumentation.

Configuring Metrics and Event emission
======================================

The following configurations are used for metrics and event emission:

|Configuration Key                | Definition           | Default        |
|---------------------------------|----------------------|----------------|
|metrics.enabled                  | Whether metrics are enabled. If false, will not report metrics. | true |
|metrics.report.interval          | Metrics report interval in milliseconds.    | 30000 |
|metrics.reporting.file.enabled   | Whether metrics will be reported to a file. | false |
|metrics.log.dir                  | If file enabled, the directory where metrics will be written. If missing, will not report to file. | N/A |
|metrics.reporting.kafka.enabled  | Whether metrics will be reported to Kafka. | false |
|metrics.reporting.kafka.brokers  | Kafka brokers for Kafka metrics emission.  | N/A   |
|metrics.reporting.kafka.topic.metrics | Kafka topic where metrics (but not events) will be reported. | N/A   |
|metrics.reporting.kafka.topic.events  | Kafka topic where events (but not metrics) will be reported. | N/A   |
|metrics.reporting.kafka.format   | Format of metrics / events emitted to Kafka. (Options: json, avro) | json |
|metrics.reporting.kafka.avro.use.schema.registry | Whether to use a schema registry for Kafka emitting. | false |
|kafka.schema.registry.url        | If using schema registry, the url of the schema registry. | N/A   |
|metrics.reporting.jmx.enabled    | Whether to report metrics to JMX.      | false  |
|metrics.reporting.custom.builders | Comma-separated list of classes for custom metrics reporters. (See [Custom Reporters](Metrics-for-Gobblin-ETL#custom-reporters)) |    |

 
Operational Metrics
===================

Each construct in a Gobblin ETL run computes metrics regarding it's performance / progress. Each metric is tagged by default with the following tags:

* jobName: Gobblin generated name for the job.
* jobId: Gobblin generated id for the job.
* clusterIdentifier: string identifier the cluster / host where the job was run. Obtained from resource manager, job tracker, or the name of the host.
* taskId: Gobblin generated id for the task that generated the metric.
* construct: construct type that generated the metric (e.g. extractor, converter, etc.)
* class: specific class of the construct that generated the metric.
* finalMetricReport: metrics are emitted regularly. Sometimes it is useful to select only the last report from each context. To aid with this, some reporters will add this tag with value "true" only to the final report from a metric context.

This is the list of operational metrics implemented by default, grouped by construct.

Extractor Metrics
-----------------
* gobblin.extractor.records.read: meter for records read.
* gobblin.extractor.records.failed: meter for records failed to read.
* gobblin.extractor.extract.time: timer for reading of records.

Converter Metrics
-----------------
* gobblin.converter.records.in: meter for records going into the converter.
* gobblin.converter.records.out: meter for records outputted by the converter.
* gobblin.converter.records.failed: meter for records that failed to be converted.
* gobblin.converter.convert.time: timer for conversion time of each record.

Fork Operator Metrics
---------------------
* gobblin.fork.operator.records.in: meter for records going into the fork operator.
* gobblin.fork.operator.forks.out: meter for records going out of the fork operator (each record is counted once for each fork it is emitted to).
* gobblin.fork.operator.fork.time: timer for forking of each record.

Row Level Policy Metrics
------------------------
* gobblin.qualitychecker.records.in: meter for records going into the row level policy.
* gobblin.qualitychecker.records.passed: meter for records passing the row level policy check.
* gobblin.qualitychecker.records.failed: meter for records failing the row level policy check.
* gobblin.qualitychecker.check.time: timer for row level policy checking of each record.

Data Writer Metrics
-------------------
* gobblin.writer.records.in: meter for records requested to be written.
* gobblin.writer.records.written: meter for records actually written.
* gobblin.writer.records.failed: meter for records failed to be written.
* gobblin.writer.write.time: timer for writing each record.

Runtime Events
==============

The Gobblin ETL runtime emits events marking its progress. All events have the following metadata:

* jobName: Gobblin generated name for the job.
* jobId: Gobblin generated id for the job.
* clusterIdentifier: string identifier the cluster / host where the job was run. Obtained from resource manager, job tracker, or the name of the host.
* taskId: Gobblin generated id for the task that generated the metric (if applicable).

This is the list of events that are emitted by the Gobblin runtime:

Job Progression Events
----------------------

* LockInUse: emitted if a job fails because it fails to get a lock.
* WorkUnitsMissing: emitted if a job exits because source failed to get work units.
* WorkUnitsEmpty: emitted if a job exits because there were no work units to process.
* WorkUnitsCreated: emitted when workunits are created for a task. Metadata: workUnitsCreated(Number of bin-packed workunits created).
* TasksSubmitted: emitted when tasks are submitted for execution. Metadata: tasksCount(number of tasks submitted).
* TaskFailed: emitted when a task fails. Metadata: taskId(id of the failed task).
* Job_Successful: emitted at the end of a successful job.
* Job_Failed: emitted at the end of a failed job.

Job Timing Events
-----------------
These events give information on timing on certain parts of the execution. Each timing event contains the following metadata:

* startTime: timestamp when the timed processing started.
* endTime: timestamp when the timed processing finished.
* durationMillis: duration in milliseconds of the timed processing.
* eventType: always "timingEvent" for timing events.

The following timing events are emitted:

* FullJobExecutionTimer: times the entire job execution.
* WorkUnitsCreationTimer: times the creation of work units.
* WorkUnitsPreparationTime: times the preparation of work units.
* JobRunTimer: times the actual running of job (i.e. processing of all work units).
* JobCommitTimer: times the committing of work units.
* JobCleanupTimer: times the job cleanup.
* JobLocalSetupTimer: times the setup of a local job.
* JobMrStagingDataCleanTimer: times the deletion of staging directories from previous work units (MR mode).
* JobMrDistributedCacheSetupTimer: times the setting up of distributed cache (MR mode).
* JobMrSetupTimer: times the setup of the MR job (MR mode).
* JobMrRunTimer: times the execution of the MR job (MR mode).

Customizing Instrumentation
===========================

Custom constructs
-----------------
When using a custom construct (for example a custom extractor for your data source), you will get the above mentioned instrumentation for free. However, you may want to implement additional metrics. To aid with this, instead of extending the usual class Extractor, you can extend the class `gobblin.instrumented.extractor.InstrumentedExtractor`. Similarly, for each construct there is an instrumented version that allows extension of the default metrics ([InstrumentedExtractor](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/extractor/InstrumentedExtractor.java), [InstrumentedConverter](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/converter/InstrumentedConverter.java), [InstrumentedForkOperator](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/fork/InstrumentedForkOperator.java), [InstrumentedRowLevelPolicy](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/qualitychecker/InstrumentedRowLevelPolicy.java), and [InstrumentedDataWriter](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/writer/InstrumentedDataWriter.java)).

All of the instrumented constructs have Javadoc providing with additional information. In general, when extending an instrumented construct, you will have to implement a different method. For example, when extending an InstrumentedExtractor, instead of implementing `readRecord`, you will implement `readRecordImpl`. To make this clearer for the user, implementing `readRecord` will throw a compilation error, and the javadoc of each method specifies the method that should be implemented.

### Instrumentable Interface

Instrumented constructs extend the interface [Instrumentable](https://github.com/apache/incubator-gobblin/blob/master/gobblin-core-base/src/main/java/org/apache/gobblin/instrumented/Instrumentable.java). It contains the following methods:

* `getMetricContext()`: get the default metric context generated for that instance of the construct, with all the appropriate tags. Use this metric context to create any additional metrics.
* `isInstrumentationEnabled()`: returns true if instrumentation is enabled.
* `switchMetricsContext(List<Tag<?>>)`: switches the default metric context returned by `getMetricContext()` to a metric context containing the supplied tags. All default metrics will be reported to the new metric context. This method is useful when the state of a construct changes during the execution, and the user desires to reflect that in the emitted tags (for example, Kafka extractor can handle multiple topics in the same extractor, and we want to reflect this in the metrics).
* `switchMetricContext(MetricContext)`: similar to the above method, but uses the supplied metric context instead of generating a new metric context. It is the responsibility of the caller to ensure the new metric context has the correct tags and parent.

The following method can be re-implemented by the user:
* `generateTags(State)`: this method should return a list of tags to use for metric contexts created for this construct. If overriding this method, it is always a good idea to call `super()` and only append tags to this list.

### Callback Methods

Instrumented constructs have a set of callback methods that are called at different points in the processing of each record, and which can be used to update metrics. For example, the `InstrumentedExtractor` has the callbacks `beforeRead()`, `afterRead(D, long)`, and `onException(Exception)`. The javadoc for the instrumented constructs has further descriptions for each callback. Users should always call `super()` when overriding this callbacks, as default metrics depend on that.

Custom Reporters
----------------

Besides the reporters implemented by default (file, Kafka, and JMX), users can add custom reporters to the classpath and instruct Gobblin to use these reporters. To do this, users should extend the interface [CustomReporterFactory](https://github.com/apache/incubator-gobblin/blob/master/gobblin-metrics-libs/gobblin-metrics-base/src/main/java/org/apache/gobblin/metrics/CustomReporterFactory.java), and specify a comma-separated list of CustomReporterFactory classes in the configuration key `metrics.reporting.custom.builders`.

Gobblin will automatically search for these CustomReporterFactory implementations, instantiate each one with a parameter-less constructor, and then call the method `newScheduledReporter(MetricContext, Properties)`, where the properties contain all of the input configurations supplied to Gobblin. Gobblin will then manage this `ScheduledReporter`.
