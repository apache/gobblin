/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.reporter;

import java.io.Closeable;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;

import javax.annotation.Nullable;

import gobblin.metrics.Event;
import gobblin.metrics.MetricContext;
import gobblin.metrics.notify.EventNotification;
import gobblin.metrics.notify.Notification;


/**
 * Abstract class for reporting {@link gobblin.metrics.Event}s at a fixed schedule.
 *
 * <p>
 *   Subclasses should implement {@link #reportEventQueue} to emit the events to the sink. Events will only be
 *   reported once, and then removed from the event queue.
 * </p>
 */
public abstract class EventReporter extends ScheduledReporter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventReporter.class);

  private final Queue<Event> reportingQueue;
  protected final Closer closer;

  public EventReporter(Builder builder) {
    super(builder.context, builder.name, builder.filter, builder.rateUnit, builder.durationUnit);

    this.closer = Closer.create();

    builder.context.addNotifyTarget(new Function<Notification, Void>() {
      @Nullable
      @Override
      public Void apply(Notification notification) {
        notificationCallback(notification);
        return null;
      }
    });
    this.reportingQueue = Queues.newConcurrentLinkedQueue();
  }

  /**
   * Callback used by the {@link gobblin.metrics.MetricContext} to notify the object of a new
   * {@link gobblin.metrics.Event}.
   * @param notification {@link gobblin.metrics.notify.Notification} to process.
   */
  public void notificationCallback(Notification notification) {
    if(notification instanceof EventNotification) {
      addEventToReportingQueue(((EventNotification) notification).event);
    }
  }

  /**
   * Add {@link gobblin.metrics.Event} to the events queue.
   * @param event {@link gobblin.metrics.Event} to add to queue.
   */
  public void addEventToReportingQueue(Event event) {
    this.reportingQueue.add(event);
  }

  /**
   * Report all {@link gobblin.metrics.Event}s in the queue.
   */
  @Override
  public void report() {
    reportEventQueue(this.reportingQueue);
  }

  /**
   * Emit all {@link gobblin.metrics.Event} in queue.
   * @param queue {@link java.util.Queue} containing {@link gobblin.metrics.Event}s that should be emitted.
   */
  public abstract void reportEventQueue(Queue<Event> queue);

  /**
   * NOOP because {@link com.codahale.metrics.ScheduledReporter} requires this method implemented.
   */
  @Override
  public final void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    //NOOP
  }

  /**
   * Builder for {@link EventReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> {
    protected MetricContext context;
    protected String name;
    protected MetricFilter filter;
    protected TimeUnit rateUnit;
    protected TimeUnit durationUnit;
    protected Map<String, String> tags;

    protected Builder(MetricContext context) {
      this.context = context;
      this.name = "MetricReportReporter";
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    protected abstract T self();

  }

  @Override
  public void close() {
    try {
      this.closer.close();
    } catch(Exception e) {
      LOGGER.warn("Exception when closing KafkaReporter", e);
    } finally {
      super.close();
    }
  }
}
