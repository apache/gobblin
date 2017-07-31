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
package org.apache.gobblin.metrics.test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.notification.EventNotification;
import org.apache.gobblin.metrics.notification.Notification;

/**
 * A class to help with testing metrics. It provides asserts on a {@link MetricContext}
 */
public class MetricsAssert implements Function<Notification, Void> {
  private final MetricContext _metricContext;
  private final LinkedBlockingQueue<GobblinTrackingEvent> _events = new LinkedBlockingQueue<>();

  public MetricsAssert(MetricContext metricContext) {
    _metricContext = metricContext;
    _metricContext.addNotificationTarget(this);
  }

  /** {@inheritDoc} */
  @Override public Void apply(Notification input) {
    if (input instanceof EventNotification) {
      _events.offer(((EventNotification)input).getEvent());
    }
    return null;
  }

  public MetricContext getMetricContext() {
    return _metricContext;
  }

  public void assertEvent(Predicate<GobblinTrackingEvent> predicate, long timeout,
                          TimeUnit timeUnit) throws TimeoutException, InterruptedException {
    GobblinTrackingEvent gte = timeout > 0 ? _events.poll(timeout, timeUnit) : _events.take();
    if (null == gte) {
      throw new TimeoutException();
    }
    if (!predicate.apply(gte)) {
      throw new AssertionError("Event predicate mismatch: " + gte);
    }
  }

  public void assertEvent(Predicate<GobblinTrackingEvent> predicate) throws InterruptedException {
    try {
      assertEvent(predicate, 0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new Error("This should never happen");
    }
  }

  public static Predicate<GobblinTrackingEvent> eqEventName(final String expectedName) {
    return new Predicate<GobblinTrackingEvent>() {
      @Override public boolean apply(@Nonnull GobblinTrackingEvent input) {
        return input.getName().equals(expectedName);
      }
    };
  }

  public static Predicate<GobblinTrackingEvent> eqEventNamespace(final String expectedNamespace) {
    return new Predicate<GobblinTrackingEvent>() {
      @Override public boolean apply(@Nonnull GobblinTrackingEvent input) {
        return input.getNamespace().equals(expectedNamespace);
      }
    };
  }

  public static Predicate<GobblinTrackingEvent> eqEventMetdata(final String metadataKey,
                                                        final String metadataValue) {
    return new Predicate<GobblinTrackingEvent>() {
      @Override public boolean apply(@Nonnull GobblinTrackingEvent input) {
        return input.getMetadata().get(metadataKey).equals(metadataValue);
      }
    };
  }

  public static Predicate<GobblinTrackingEvent> hasEventMetdata(final String metadataKey) {
    return new Predicate<GobblinTrackingEvent>() {
      @Override public boolean apply(@Nonnull GobblinTrackingEvent input) {
        return input.getMetadata().containsKey(metadataKey);
      }
    };
  }

}
