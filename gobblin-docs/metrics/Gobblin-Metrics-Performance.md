Table of Contents
-----------------

[TOC]

Generalities
============
These are the main resources used by Gobblin Metrics:

* CPU time for updating metrics: scales with number of metrics and frequency of metric update
* CPU time for metric emission and lifecycle management: scales with number of metrics and frequency of emission
* Memory for storing metrics: scales with number of metrics and metric contexts
* I/O for reporting metrics: scales with number of metrics and frequency of emission
* External resources for metrics emission (e.g. HDFS space, Kafka queue space, etc.): scales with number of metrics and frequency of emission

This page focuses on the CPU time for updating metrics, as these updates are usually in the critical performance path of an application. Each metric requires bounded memory, and having a few metrics should have no major effect on memory usage. Metrics and Metric Contexts are cleaned when no longer needed to further reduce this impact. Resources related to metric emission can always be reduced by reporting fewer metrics or decreasing the reporting frequency when necessary.

How to interpret these numbers
==============================
This document provides maximum QPS achievable by Gobblin Metrics. If the application attempts to update metrics at a higher rate than this, the metrics will effectively throttle the application. If, on the other hand, the application only updates metrics at 10% or less of the maximum QPS, the performance impact of Gobblin Metrics should be minimal.

### What if I need larger QPS?
If your application needs larger QPS, the recommendation is to batch metrics updates. Counters and Meters offer the option to increase their values by multiple units at a time. Histograms and Timers don't offer this option, but for very high throughput applications, randomly registering for example only 10% of the values will not affect statistics significantly (although you will have to adjust timer and histogram counts manually).

Update Metrics Performance
==========================
Metric updates are the most common interaction with Gobblin Metrics in an application. Every time a counter is increased, a meter is marked, or entries are added to histograms and timers, an update happens. As such, metric updates are the most likely to impact application performance.

We measured the max number of metric updates that can be executed per second. The performance of different metric types is different. Also, the performance of metrics depends on the depth in the Metric Context tree at which they are created. Metrics in the Root Metric Context are the fastest, while metrics deep in the tree are slower because they have to update all ancestors as well. The following table shows reference max QPS in updates per second as well as the equivalent single update delay in nanoseconds for each metric type in a i7 processor:

| Metric | Root level | Depth: 1 | Depth: 2 | Depth: 3 |
|--------|------------|----------|----------|----------|
| Counter | 76M (13ns) | 39M (25ns) | 29M (34ns) | 24M (41ns) |
| Meter | 11M (90ns) | 7M (142ns) | 4.5M (222ns) | 3.5M (285ns) |
| Histogram | 2.4M (416ns) | 2.4M (416ns) | 1.8M (555ns) | 1.3M (769ns) |
| Timer | 1.4M (714ns) | 1.4M (714ns) | 1M (1us) | 1M (1us) |

Multiple metric updates per iteration
-------------------------------------
If a single thread updates multiple metrics, the average delay for metric updates will be the sum of the delays of each metric independently. For example, if each iteration the application is updating two counters, one timer, and one histogram at the root metric context level, the total delay will be `13ns + 13ns + 416ns + 714ns = 1156ns` for a max QPS of `865k`.

Multi-threading
---------------
Updating metrics with different names can be parallelized efficiently, e.g. different threads updating metrics with different names will not interfere with each other. However, multiple threads updating metrics with the same names will interfere with each other, as the updates of common ancestor metrics are synchronized (to provide with auto-aggregation). In experiments we observed that updating metrics with the same name from multiple threads increases the maximum QPS sub-linearly, saturating at about 3x the single threaded QPS, i.e. the total QPS of metrics updates across any number of threads will not go about 3x the numbers shown in the table above.

On the other hand, if each thread is updating multiple metrics, the updates might interleave with each other, potentially increasing the max total QPS. In the example with two counters, one timer, and one histogram, one thread could be updating the timer while another could be updating the histogram, reducing interference, but never exceeding the max QPS of the single most expensive metric. Note that there is no optimization in code to produce this interleaving, it is merely an effect of synchronization, so the effect might vary.

Running Performance Tests
-------------------------
To run the performance tests
```bash
cd gobblin-metrics
../gradlew performance
```

After finishing, it should create a TestNG report at `build/gobblin-metrics/reports/tests/packages/gobblin.metrics.performance.html`. Nicely printed performance results are available on the Output tab. 
