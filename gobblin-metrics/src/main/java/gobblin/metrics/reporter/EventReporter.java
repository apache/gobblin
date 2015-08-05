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
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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
import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.notification.EventNotification;
import gobblin.metrics.notification.Notification;
import gobblin.util.ExecutorsUtils;


/**
 * Abstract class for reporting {@link gobblin.metrics.GobblinTrackingEvent}s at a fixed schedule.
 *
 * <p>
 *   Subclasses should implement {@link #reportEventQueue} to emit the events to the sink. Events will only be
 *   reported once, and then removed from the event queue.
 * </p>
 */
public abstract class EventReporter extends ScheduledReporter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventReporter.class);
  private static final int QUEUE_CAPACITY = 100;

  private final Queue<GobblinTrackingEvent> reportingQueue;
  private final ExecutorService immediateReportExecutor;
  protected final Closer closer;

  public EventReporter(Builder builder) {
    super(builder.context, builder.name, builder.filter, builder.rateUnit, builder.durationUnit);

    this.closer = Closer.create();
    this.immediateReportExecutor = MoreExecutors.
        getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(1,
            ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("EventReporter-" + builder.name + "-%d"))),
            5, TimeUnit.MINUTES);

    builder.context.addNotificationTarget(new Function<Notification, Void>() {
      @Nullable
      @Override
      public Void apply(Notification notification) {
        notificationCallback(notification);
        return null;
      }
    });
    this.reportingQueue = Queues.newLinkedBlockingQueue(QUEUE_CAPACITY);
  }

  /**
   * Callback used by the {@link gobblin.metrics.MetricContext} to notify the object of a new
   * {@link gobblin.metrics.GobblinTrackingEvent}.
   * @param notification {@link gobblin.metrics.notification.Notification} to process.
   */
  public void notificationCallback(Notification notification) {
    if(notification instanceof EventNotification) {
      addEventToReportingQueue(((EventNotification) notification).getEvent());
    }
  }

  /**
   * Add {@link gobblin.metrics.GobblinTrackingEvent} to the events queue.
   * @param event {@link gobblin.metrics.GobblinTrackingEvent} to add to queue.
   */
  public void addEventToReportingQueue(GobblinTrackingEvent event) {
    if(this.reportingQueue.size() > QUEUE_CAPACITY * 2 / 3) {
      immediatelyScheduleReport();
    }
    this.reportingQueue.add(event);
  }

  /**
   * Report all {@link gobblin.metrics.GobblinTrackingEvent}s in the queue.
   */
  @Override
  public void report() {
    reportEventQueue(this.reportingQueue);
  }

  /**
   * Emit all {@link gobblin.metrics.GobblinTrackingEvent} in queue.
   * @param queue {@link java.util.Queue} containing {@link gobblin.metrics.GobblinTrackingEvent}s that should be emitted.
   */
  public abstract void reportEventQueue(Queue<GobblinTrackingEvent> queue);

  /**
   * NOOP because {@link com.codahale.metrics.ScheduledReporter} requires this method implemented.
   */
  @Override
  public final void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    //NOOP
  }

  private void immediatelyScheduleReport() {
    this.immediateReportExecutor.submit(new Runnable() {
      @Override
      public void run() {
        report();
      }
    });
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
