Table of Contents
-----------------

[TOC]

Gobblin Metrics is a metrics library for emitting metrics and events instrumenting java applications. 
Metrics and events are easy to use and enriched with tags. Metrics allow full granularity, auto-aggregation, and configurable 
reporting schedules. Gobblin Metrics is based on [Dropwizard Metrics](http://metrics.dropwizard.io/), enhanced to better support 
modular applications (by providing hierarchical, auto-aggregated metrics) and their monitoring / auditing.

Quick Start
===========

The following code excerpt shows the functionality of Gobblin Metrics.

```java
// ========================================
// METRIC CONTEXTS
// ========================================

// Create a Metric context with a Tag
MetricContext context = MetricContext.builder("MyMetricContext").addTag(new Tag<Integer>("key", value)).build();
// Create a child metric context. It will automatically inherit tags from parent.
// All metrics in the child context will be auto-aggregated in the parent context.
MetricContext childContext = context.childBuilder("childContext").build();

// ========================================
// METRICS
// ========================================

// Create a reporter for metrics. This reporter will write metrics to STDOUT.
OutputStreamReporter.Factory.newBuilder().build(new Properties());
// Start all metric reporters.
RootMetricContext.get().startReporting();

// Create a counter.
Counter counter = childContext.counter("my.counter.name");
// Increase the counter. The next time metrics are reported, "my.counter.name" will be reported as 1.
counter.inc();

// ========================================
// EVENTS
// ========================================

// Create an reporter for events. This reporter will write events to STDOUT.
ScheduledReporter eventReporter = OutputStreamEventReporter.forContext(context).build();
eventReporter.start();

// Create an event submitter, can include default metadata.
EventSubmitter eventSubmitter = new EventSubmitter.Builder(context, "events.namespace").addMetadata("metadataKey", "value").build();
// Submit an event. Its metadata will contain all tags in context, all metadata in eventSubmitter,
// and any additional metadata specified in the call.
// This event will be displayed the next time the event reporter flushes.
eventSubmitter.submit("EventName", "additionalMetadataKey", "value");
```

Metric Contexts
===============

A metric context is a context from which users can emit metrics and events. These contexts contain a set of tags, each tag 
being a key-value pair. Contexts are hierarchical in nature: each context has one parent and children. They automatically 
inherit the tags of their parent, and can define or override more tags.

Generally, a metric context is associated with a specific instance of an object that should be instrumented. 
Different instances of the same object will have separate instrumentations. However, each context also aggregates 
all metrics defined by its descendants, providing with a full range of granularities for reporting. 
With this functionality if, for example, an application has 10 different data writers,  users can monitor each writer 
individually, or all at the same time.

Metrics
=======

Metrics are used to monitor the progress of an application. Metrics are emitted regularly following a schedule and represent 
the current state of the application. The metrics supported by Gobblin Metrics are the same ones as those supported 
by [Dropwizard Metrics Core](http://metrics.dropwizard.io/3.1.0/manual/core/), adapted for tagging and auto-aggregation. 
The types supported are:

* Counter: simple long counter.
* Meter: counter with added computation of the rate at which the counter is changing.
* Histogram: stores a histogram of a value, divides all of the values observed into buckets, and reports the count for each bucket.
* Timer: a histogram for timing information.
* Gauge: simply stores a value. Gauges are not auto-aggregated because the aggregation operation is context-dependent.

Events
======

Events are fire-and-forget messages indicating a milestone in the execution of an application, 
along with metadata that can provide further information about that event (all tags of the metric context used to generate 
the event are also added as metadata).

Reporters
=========

Reporters periodically output the metrics and events to particular sinks following a configurable schedule. Events and Metrics reporters are kept separate to allow users more control in case they want to emit metrics and events to separate sinks (for example, different files). Reporters for a few sinks are implemented by default, but additional sinks can be implemented by extending the `RecursiveScheduledMetricReporter` and the `EventReporter`. Each of the included reporters has a simple builder.

The metric reporter implementations included with Gobblin Metrics are:

* OutputStreamReporter: Supports any output stream, including STDOUT and files.
* KafkaReporter: Emits metrics to a Kafka topic as Json messages.
* KafkaAvroReporter: Emits metrics to a Kafka topic as Avro messages.
* InfluxDBReporter: Emits metrics to Influx DB.
* GraphiteReporter: Emits metrics to Graphite.
* HadoopCounterReporter: Emits metrics as Hadoop counters.

The event reporter implementations included with Gobblin metrics are:

* OutputStreamEventReporter: Supports any output stream, including STDOUT and files.
* KafkaEventReporter: Emits events to Kafka as Json messages.
* KafkaEventAvroReporter: Emits events to Kafka as Avro messages.
