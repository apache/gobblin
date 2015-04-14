/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.google.common.collect.Lists;


/**
 * Unit tests for {@link BoundedBlockingRecordQueue}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class BoundedBlockingRecordQueueTest {

  private static final String METRIC_NAME_PREFIX = "test";

  private BoundedBlockingRecordQueue<Integer> boundedBlockingRecordQueue;

  @BeforeClass
  public void setUp() {
    this.boundedBlockingRecordQueue = BoundedBlockingRecordQueue.<Integer>newBuilder()
        .hasCapacity(2)
        .useTimeout(100)
        .useTimeoutTimeUnit(TimeUnit.MILLISECONDS)
        .collectStats()
        .build();
  }

  @Test
  public void testPutAndGet() throws InterruptedException {
    final List<Integer> produced = Lists.newArrayList();
    Thread producer = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < 6; i++) {
          try {
            boundedBlockingRecordQueue.put(i);
            produced.add(i);
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        }
      }
    });

    final List<Integer> consumed = Lists.newArrayList();
    Thread consumer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < 6; i++) {
            consumed.add(boundedBlockingRecordQueue.get());
          }
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    });

    producer.start();
    consumer.start();

    producer.join();
    consumer.join();

    Assert.assertEquals(produced, consumed);
    Assert.assertNull(this.boundedBlockingRecordQueue.get());
  }

  @Test(dependsOnMethods = "testPutAndGet")
  public void testQueueStats() throws InterruptedException {
    BoundedBlockingRecordQueue.QueueStats stats = this.boundedBlockingRecordQueue.stats().get();
    Assert.assertEquals(stats.queueSize(), 0);
    Assert.assertEquals(stats.fillRatio(), 0d);
    Assert.assertEquals(stats.getAttemptCount(), 7);
    Assert.assertEquals(stats.putAttemptCount(), 6);

    this.boundedBlockingRecordQueue.put(0);
    this.boundedBlockingRecordQueue.put(1);

    Assert.assertEquals(stats.queueSize(), 2);
    Assert.assertEquals(stats.fillRatio(), 1d);
    Assert.assertEquals(stats.getAttemptCount(), 7);
    Assert.assertEquals(stats.putAttemptCount(), 8);
  }

  @Test(dependsOnMethods = "testQueueStats")
  public void testRegisterAll() {
    MetricRegistry metricRegistry = new MetricRegistry();
    this.boundedBlockingRecordQueue.stats().get().registerAll(metricRegistry, METRIC_NAME_PREFIX);
    Map<String, Gauge> gauges = metricRegistry.getGauges();
    Assert.assertEquals(gauges.size(), 2);
    Assert.assertEquals(
        gauges.get(MetricRegistry.name(METRIC_NAME_PREFIX, BoundedBlockingRecordQueue.QueueStats.QUEUE_SIZE))
            .getValue(), 2);
    Assert.assertEquals(
        gauges.get(MetricRegistry.name(METRIC_NAME_PREFIX, BoundedBlockingRecordQueue.QueueStats.FILL_RATIO))
            .getValue(), 1d);
    Assert.assertEquals(metricRegistry.getMeters().size(), 2);
    Assert.assertEquals(metricRegistry
        .meter(MetricRegistry.name(METRIC_NAME_PREFIX, BoundedBlockingRecordQueue.QueueStats.GET_ATTEMPT_RATE))
        .getCount(), 7);
    Assert.assertEquals(metricRegistry
        .meter(MetricRegistry.name(METRIC_NAME_PREFIX, BoundedBlockingRecordQueue.QueueStats.PUT_ATTEMPT_RATE))
        .getCount(), 8);
  }

  @AfterClass
  public void tearDown() throws InterruptedException {
    this.boundedBlockingRecordQueue.clear();
    Assert.assertNull(this.boundedBlockingRecordQueue.get());
  }
}
