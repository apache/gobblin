/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.metrics.reporter;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

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
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.notification.EventNotification;
import org.apache.gobblin.metrics.notification.Notification;
import org.apache.gobblin.util.ExecutorsUtils;

import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_ID;
import static org.apache.gobblin.metrics.event.TaskEvent.METADATA_TASK_ID;

/**
 * Abstract class for reporting {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s at a fixed schedule.
 *
 * <p>
 *   Subclasses should implement {@link #reportEventQueue} to emit the events to the sink. Events will only be
 *   reported once, and then removed from the event queue.
 * </p>
 */
@Slf4j
public abstract class EventReporter extends ScheduledReporter implements Closeable {

  protected static final Joiner JOINER = Joiner.on('.').skipNulls();
  protected static final String METRIC_KEY_PREFIX = "gobblin.metrics";
  protected static final String EVENTS_QUALIFIER = "events";
  private static final Logger LOGGER = LoggerFactory.getLogger(EventReporter.class);
  private static final int QUEUE_CAPACITY = 100;
  private static final String NULL_STRING = "null";

  private final MetricContext metricContext;
  private final BlockingQueue<GobblinTrackingEvent> reportingQueue;
  private final ExecutorService immediateReportExecutor;
  private final UUID notificationTargetKey;
  protected final Closer closer;

  public EventReporter(Builder builder) {
    super(builder.context, builder.name, builder.filter, builder.rateUnit, builder.durationUnit);

    this.closer = Closer.create();
    this.immediateReportExecutor = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(1,
            ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("EventReporter-" + builder.name + "-%d"))),
        5, TimeUnit.MINUTES);

    this.metricContext = builder.context;
    this.notificationTargetKey = builder.context.addNotificationTarget(new Function<Notification, Void>() {
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
   * Callback used by the {@link org.apache.gobblin.metrics.MetricContext} to notify the object of a new
   * {@link org.apache.gobblin.metrics.GobblinTrackingEvent}.
   * @param notification {@link org.apache.gobblin.metrics.notification.Notification} to process.
   */
  public void notificationCallback(Notification notification) {
    if (notification instanceof EventNotification) {
      addEventToReportingQueue(((EventNotification) notification).getEvent());
    }
  }

  /**
   * Add {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to the events queue.
   * @param event {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to add to queue.
   */
  public void addEventToReportingQueue(GobblinTrackingEvent event) {
    if (this.reportingQueue.size() > QUEUE_CAPACITY * 2 / 3) {
      immediatelyScheduleReport();
    }
    try {
      if (!this.reportingQueue.offer(sanitizeEvent(event), 10, TimeUnit.SECONDS)) {
        log.error("Enqueuing of event %s at reporter with class %s timed out. Sending of events is probably stuck.",
            event, this.getClass().getCanonicalName());
      }
    } catch (InterruptedException ie) {
      log.warn(String.format("Enqueuing of event %s at reporter with class %s was interrupted.", event,
          this.getClass().getCanonicalName()), ie);
    }
  }

  /**
   * Report all {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s in the queue.
   */
  @Override
  public void report() {
    reportEventQueue(this.reportingQueue);
  }

  /**
   * Emit all {@link org.apache.gobblin.metrics.GobblinTrackingEvent} in queue.
   * @param queue {@link java.util.Queue} containing {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s that should be emitted.
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

  /**
   * Constructs the metric key to be emitted.
   * The actual event name is enriched with the current job and task id to be able to keep track of its origin
   *
   * @param metadata metadata of the actual {@link GobblinTrackingEvent}
   * @param eventName name of the actual {@link GobblinTrackingEvent}
   * @return prefix of the metric key
   */
  protected String getMetricName(Map<String, String> metadata, String eventName) {
    return JOINER.join(METRIC_KEY_PREFIX, metadata.get(METADATA_JOB_ID), metadata.get(METADATA_TASK_ID),
        EVENTS_QUALIFIER, eventName);
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
      this.metricContext.removeNotificationTarget(this.notificationTargetKey);
      report();
      this.closer.close();
    } catch (Exception e) {
      LOGGER.warn("Exception when closing EventReporter", e);
    } finally {
      super.close();
    }
  }

  private static GobblinTrackingEvent sanitizeEvent(GobblinTrackingEvent event) {
    Map<String, String> newMetadata = Maps.newHashMap();
    for (Map.Entry<String, String> metadata : event.getMetadata().entrySet()) {
      newMetadata.put(metadata.getKey() == null ? NULL_STRING : metadata.getKey(),
          metadata.getValue() == null ? NULL_STRING : metadata.getValue());
    }
    event.setMetadata(newMetadata);
    return event;
  }
}
