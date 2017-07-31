Table of Contents
-----------------

[TOC]

The two best entry points for implementing custom reporters are [RecursiveScheduledMetricReporter](https://github.com/linkedin/gobblin/blob/master/gobblin-metrics/src/main/java/gobblin/metrics/reporter/RecursiveScheduledMetricReporter.java) and [EventReporter](https://github.com/linkedin/gobblin/blob/master/gobblin-metrics/src/main/java/gobblin/metrics/reporter/EventReporter.java). Each of these classes automatically schedules reporting, extracts the correct metrics, and calls a single method that must be implemented by the developer. These methods also implement builder patterns that can be extended by the developer.

In the interest of giving more control to the users, metric and event reporters are kept separate, allowing users to more easily specify separate sinks for events and metrics. However, it is possible to implement a single report that handles both events and metrics.

> It is recommended that each reporter has a constructor with signature `<init>(Properties)`. In the near future we are planning to implement auto-starting, file-configurable reporting similar to Log4j architecture, and compliant reporters will be required to have such a constructor.

Extending Builders
==================

The builder patterns implemented in the base reporters are designed to be extendable. The architecture is a bit complicated, but a subclass of the base reporters wanting to use builder patterns should follow this pattern (replacing with RecursiveScheduledMetricReporter in the case of a metrics reporter):

```java
class MyReporter extends EventReporter {

  private MyReporter(Builder<?> builder) throws IOException {
    super(builder);
    // Other initialization logic.
  }

  // Concrete implementation of extendable Builder.
  public static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static class Factory {
    /**
     * Returns a new {@link MyReporter.Builder} for {@link MyReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link org.apache.gobblin.metrics.MetricContext} to report
     * @return MyReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link MyReporter}.
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>>
      extends EventReporter.Builder<T> {

    // Additional instance variables needed to construct MyReporter.
    private int myBuilderVariable;

    protected Builder(MetricContext context) {
      super(context);
      this.myBuilderVariable = 0;
    }

    /**
     * Set myBuilderVariable.
     */
    public T withMyBuilderVariable(int value) {
      this.myBuilderVariable = value;
      return self();
    }

    // Other setters for Builder variables.

    /**
     * Builds and returns {@link MyReporter}.
     */
    public MyReporter build() throws IOException {
      return new MyReporter(this);
    }

  }
}
```

This pattern allows users to simply call
```java
MyReporter reporter = MyReporter.Factory.forContext(context).build();
```
to generate an instance of the reporter. Additionally, if you want to further extend MyReporter, following the exact same pattern except extending MyReporter instead of EventReporter will work correctly (which would not be true with standard Builder pattern).

Metric Reporting
================

Developers should extend `RecursiveScheduledMetricReporter` and implement the method `RecursiveScheduledMetricReporter#report`. The base class will call report when appropriate with the list of metrics, separated by type, and tags that should be reported.

Event Reporting
===============

Developers should extend `EventReporter` and implement the method `EventReporter#reportEventQueue(Queue<GobblinTrackingEvent>)`. The base class will call this method with a queue of all events to report as needed.

Other Reporters
===============

It is also possible to implement a reporter without using the suggested classes. Reporters are recommended, but not required, to extend the interface `Reporter`. Reporters can use the public methods of `MetricContext` to navigate the Metric Context tree, query metrics, and register for notifications.
