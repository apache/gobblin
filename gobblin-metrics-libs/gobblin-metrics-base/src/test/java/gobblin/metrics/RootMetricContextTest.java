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

package gobblin.metrics;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.lang.ref.WeakReference;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;

import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;

import gobblin.metrics.callback.NotificationStore;
import gobblin.metrics.notification.MetricContextCleanupNotification;
import gobblin.metrics.notification.NewMetricContextNotification;
import gobblin.metrics.notification.Notification;
import gobblin.metrics.test.ContextStoreReporter;


/**
 * Tests for {@link gobblin.metrics.RootMetricContext}
 */
public class RootMetricContextTest {

  private static final Cache<String, int[]> CACHE = CacheBuilder.newBuilder().softValues().build();

  @BeforeMethod
  public void setUp() throws Exception {
    System.gc();
    RootMetricContext.get().clearNotificationTargets();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    CACHE.invalidateAll();
    System.gc();
    RootMetricContext.get().clearNotificationTargets();
  }

  @Test
  public void testGet() throws Exception {
    Assert.assertNotNull(RootMetricContext.get());
    Assert.assertEquals(RootMetricContext.get(), RootMetricContext.get());
    Assert.assertEquals(RootMetricContext.get().getName(), RootMetricContext.ROOT_METRIC_CONTEXT);
  }

  @Test
  public void testReporterCanBeAddedToStartedContext() throws Exception {
    RootMetricContext.get().startReporting();

    ContextStoreReporter reporter = new ContextStoreReporter("testReporter", ConfigFactory.empty());
    Assert.assertTrue(reporter.isStarted());

    RootMetricContext.get().stopReporting();
  }

  @Test
  public void testMetricContextLifecycle() throws Exception {

    String name = UUID.randomUUID().toString();
    NotificationStore store = new NotificationStore(new ContextNamePredicate(name));
    RootMetricContext.get().addNotificationTarget(store);

    // Create a new metric context
    MetricContext metricContext = MetricContext.builder(name).build();
    WeakReference<MetricContext> contextWeakReference = new WeakReference<MetricContext>(metricContext);
    InnerMetricContext innerMetricContext = metricContext.getInnerMetricContext();
    WeakReference<InnerMetricContext> innerMetricContextWeakReference =
        new WeakReference<InnerMetricContext>(innerMetricContext);
    innerMetricContext = null;
    // Check that existence of a reporter does not prevent GC
    ContextStoreReporter reporter = new ContextStoreReporter("testReporter", ConfigFactory.empty());

    // Check that metric context is a child of root metric context
    Assert.assertTrue(RootMetricContext.get().getChildContextsAsMap().containsKey(name));
    Assert.assertEquals(RootMetricContext.get().getChildContextsAsMap().get(name), metricContext);

    // Check that notification on new metric context was generated
    Assert.assertEquals(store.getNotificationList().size(), 1);
    Assert.assertEquals(store.getNotificationList().get(0).getClass(), NewMetricContextNotification.class);
    Assert.assertEquals(((NewMetricContextNotification) store.getNotificationList().get(0)).getMetricContext(),
        metricContext);
    store.getNotificationList().clear();

    // Create a counter
    ContextAwareCounter counter1 = metricContext.contextAwareCounter("textCounter1");

    // If losing reference of counter, should not be GCed while context is present
    WeakReference<ContextAwareCounter> counterWeakReference1 = new WeakReference<ContextAwareCounter>(counter1);
    counter1 = null;
    ensureNotGarbageCollected(counterWeakReference1);

    // Create some more metrics
    ContextAwareCounter counter2 = metricContext.contextAwareCounter("testCounter");
    WeakReference<ContextAwareCounter> counterWeakReference2 = new WeakReference<ContextAwareCounter>(counter2);
    ContextAwareMeter meter = metricContext.contextAwareMeter("testMeter");
    WeakReference<ContextAwareMeter> meterWeakReference = new WeakReference<ContextAwareMeter>(meter);
    meter.mark();
    ContextAwareHistogram histogram = metricContext.contextAwareHistogram("testHistogram");
    WeakReference<ContextAwareHistogram> histogramWeakReference = new WeakReference<ContextAwareHistogram>(histogram);
    ContextAwareTimer timer = metricContext.contextAwareTimer("testTimer");
    WeakReference<ContextAwareTimer> timerWeakReference = new WeakReference<ContextAwareTimer>(timer);


    // If losing reference to context, should not be GCed while reference to metric is present
    metricContext = null;
    ensureNotGarbageCollected(contextWeakReference);
    ensureNotGarbageCollected(counterWeakReference2);
    ensureNotGarbageCollected(meterWeakReference);
    ensureNotGarbageCollected(timerWeakReference);
    ensureNotGarbageCollected(histogramWeakReference);

    // After losing reference to context and all metrics, context and all metrics should be GCed
    store.getNotificationList().clear();
    reporter.getReportedContexts().clear();
    counter2 = null;
    meter = null;
    histogram = null;
    timer = null;

    ensureGarbageCollected(contextWeakReference);
    ensureGarbageCollected(counterWeakReference1);
    ensureGarbageCollected(counterWeakReference2);
    ensureGarbageCollected(meterWeakReference);
    ensureGarbageCollected(timerWeakReference);
    ensureGarbageCollected(histogramWeakReference);

    // Inner metric context should not be GCed
    ensureNotGarbageCollected(innerMetricContextWeakReference);

    // Notification on removal of metric context should be available
    int maxWait = 10;
    while(store.getNotificationList().isEmpty() && maxWait > 0) {
      Thread.sleep(1000);
      maxWait--;
    }
    Assert.assertEquals(store.getNotificationList().size(), 1);
    Assert.assertEquals(store.getNotificationList().get(0).getClass(), MetricContextCleanupNotification.class);
    Assert.assertEquals(((MetricContextCleanupNotification) store.getNotificationList().get(0)).getMetricContext(),
        innerMetricContextWeakReference.get());

    // Reporter should have attempted to report metric context
    Assert.assertEquals(reporter.getReportedContexts().size(), 1);
    Assert.assertEquals(reporter.getReportedContexts().get(0), innerMetricContextWeakReference.get());

    // Metrics in deleted metric context should still be readable
    Assert.assertEquals(innerMetricContextWeakReference.get().getCounters().size(), 2);
    Assert.assertEquals(innerMetricContextWeakReference.get().getMeters().size(), 1);
    Assert.assertEquals(innerMetricContextWeakReference.get().getTimers().size(), 2);
    Assert.assertEquals(innerMetricContextWeakReference.get().getHistograms().size(), 1);
    Assert.assertEquals(innerMetricContextWeakReference.get().getMeters().get("testMeter").getCount(), 1);

    // After clearing notification, inner metric context should be GCed
    store.getNotificationList().clear();
    reporter.getReportedContexts().clear();
    ensureGarbageCollected(innerMetricContextWeakReference);

    RootMetricContext.get().removeReporter(reporter);
  }

  private void triggerGarbageCollection(WeakReference<?> weakReference) {
    System.gc();

    // System.gc() might not clean up the object being checked, so allocate some memory and call System.gc() again
    for (int i = 0; weakReference.get() != null && i <= 10000; i++) {
      CACHE.put(Integer.toString(i), new int[10000]);

      if (i % 1000 == 0) {
        System.gc();
      }
    }
  }
  private void ensureGarbageCollected(WeakReference<?> weakReference) {
    triggerGarbageCollection(weakReference);

    Assert.assertNull(weakReference.get());
  }

  private void ensureNotGarbageCollected(WeakReference<?> weakReference) {
    triggerGarbageCollection(weakReference);

    Assert.assertNotNull(weakReference.get());
  }

  @AllArgsConstructor
  private class ContextNamePredicate implements Predicate<Notification> {

    private final String name;

    @Override
    public boolean apply(Notification input) {
      if (input instanceof NewMetricContextNotification &&
          ((NewMetricContextNotification) input).getInnerMetricContext().getName().equals(this.name)) {
        return true;
      }
      if (input instanceof MetricContextCleanupNotification &&
          ((MetricContextCleanupNotification) input).getMetricContext().getName().equals(this.name)) {
        return true;
      }
      return false;
    }
  }
}
