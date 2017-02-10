Table of Contents
-----------------

[TOC]


# Introduction

*Gobblin-modules* is a way to support customization of the gobblin-distribution build.

One of the core features of Gobblin is ability to integrate for a number of systems for data management (sources, targets, monitoring, etc.) Often this leads to inclusion of libraries specific to those systems. Sometimes, such systems also introduce incompatible changes in their APIs (e.g. Kafka 0.8 vs Kafka 0.9).

As the adoption of Gobblin grows and we see an increased number of such dependencies, it is no longer easy (or possible) to maintain a single monolithic gobblin-distribution build. This is where gobblin-modules.

# How it works

## gobblin-modules/

We are moving non-core functionality which may bring conflicting or large external dependencies to a new location: `gobblin-modules/`. This contains the collection of libraries (modules) which bring external depenencies.

For example, currently we have:

- `gobblin-kafka-08` - source, writer, metrics reporter using Kafka 0.8 API
- `gobblin-metrics-graphite` - metrics reporter to Graphite

Other libraries can refer to those modules using standard Gradle dependencies.

## Gobblin flavor

We have added a build property `gobblinFlavor` which controls what modules to be build and included in the gobblin-distribution tarball. The property can be used as follows
```
    ./gradlew -PgobblinFlavor=minimal build
```

Gobblin libraries that support customization can add build files like `gobblin-flavor-<FLAVOR>.gradle` which declare the dependencies. For example, let's look at the current `gobblin-core/gobblin-flavor-standard.gradle` :

```
dependencies {
  compile project(':gobblin-modules:gobblin-kafka-08')
}
```

That specifies that the "standard" flavor of Gobblin will include the Kafka 0.8 source, writer and metric reporter.

When one specifies the `-PgobblinFlavor=<FLAVOR>` during build time, the build script will automatically include the dependencies specified in the corresponding `gobblin-flavor-<FLAVOR>.gradle` files in any library that contains such file.

Currently, Gobblin defines 4 flavors out of the box:

- minimal - no modules
- standard - standard modules for frequently used components. This is the flavor used if none is explicitly specified
- cluster - modules for running Gobblin clusters (YARN, AWS, stand-alone)
- full - all non-conflicting modules
- custom - by default, like minimal but lets users/developers modify and customize the dependencies to be included.

Users/developers can define their own flavor files.

# Current flavors and modules

| Module           | Flavors         | Description |
|------------------|----------------|-------------|
| gobblin-azkaban | standard, full | Classes to run gobblin jobs in Azkaban |
| gobblin-aws | cluster, full | Classes to run gobblin clusters on AWS |
| gobblin-cluster | cluster, full | Generic classes for running Gobblin clusters |
| gobblin-compliance | full | Source,converters, writer for cleaning existing datasets for compliance purposes |
| gobblin-helix | full | State store implementation using Helix/ZK |
| gobblin-kafka-08 | standard, full | Source, writer and metrics reporter using Kafka 0.8 APIs |
| gobblin-kafka-09 |  | Source, writer and metrics reporter using Kafka 0.9 APIs |
| gobblin-metrics-graphite | standard, full | metrics reporter to Graphite |
| gobblin-metrics-influxdb | standard, full | metrics reporter to InfluxDB |
| gobblin-metrics-hadoop | standard, full | metrics reporter to Hadoop counters |
| gobblin-yarn | cluster, full | Classes to run gobblin clusters on YARN as a native app |
| google-ingestion | standard, full | Source/extractors for GoogleWebMaster, GoogleAnalytics, GoogleDrive |
| gobblin-azure-datalake | full | FileSystem for Azure Data lake |

Note: Some grandfathered modules may not be in the gobblin-modules/ directory yet. Typically, those are in the root directory.

# What's next

We are in the process of moving existing external dependencies out of `gobblin-core` into separate modules. To preserve backwards compatibility, we will preserve package and class names and make the "standard" flavor of `gobblin-core` depend on these modules.

In the future, new external source, writer and other dependencies are expected to be added directly to gobblin-modules/. Further, we may decide to switch modules between flavors to conrol the number of external dependencies. This will always be done with advanced notice.
