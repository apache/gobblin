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
package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;


public class KafkaIngestionHealthCheckTest {
  private EventBus eventBus;
  private CountDownLatch countDownLatch = new CountDownLatch(1);

  @BeforeClass
  public void setUp() throws IOException {
    this.eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
        SharedResourcesBrokerFactory.getImplicitBroker());
    this.eventBus.register(this);
  }

  @Subscribe
  public void handleContainerHealthCheckFailureEvent(ContainerHealthCheckFailureEvent event) {
    this.countDownLatch.countDown();
  }

  @Test
  public void testExecute()
      throws InterruptedException {
    Config config = ConfigFactory.empty().withValue(KafkaIngestionHealthCheck.KAFKA_INGESTION_HEALTH_CHECK_EXPECTED_CONSUMPTION_RATE_MBPS_KEY,
        ConfigValueFactory.fromAnyRef(5))
        .withValue(KafkaIngestionHealthCheck.KAFKA_INGESTION_HEALTH_CHECK_LATENCY_THRESHOLD_MINUTES_KEY, ConfigValueFactory.fromAnyRef(5));

    KafkaExtractorStatsTracker extractorStatsTracker = Mockito.mock(KafkaExtractorStatsTracker.class);
    Mockito.when(extractorStatsTracker.getMaxIngestionLatency(TimeUnit.MINUTES))
        .thenReturn(6L)
        .thenReturn(7L)
        .thenReturn(10L)
        .thenReturn(5L);
    Mockito.when(extractorStatsTracker.getConsumptionRateMBps())
        .thenReturn(2.0)
        .thenReturn(1.5)
        .thenReturn(2.1)
        .thenReturn(2.5);

    KafkaIngestionHealthCheck check = new KafkaIngestionHealthCheck(config, extractorStatsTracker);

    //Latency increases continuously for the first 3 calls to execute().
    check.execute();
    this.countDownLatch.await(10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(this.countDownLatch.getCount(), 1L);
    check.execute();
    this.countDownLatch.await(10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(this.countDownLatch.getCount(), 1L);
    check.execute();
    //Ensure that ContainerHealthCheckFailureEvent is posted to eventBus; countDownLatch should be back to 0.
    this.countDownLatch.await(10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(this.countDownLatch.getCount(), 0);

    //Set the countdown latch back to 1.
    this.countDownLatch = new CountDownLatch(1);
    //Latency decreases from 10 to 5. So check.execute() should not post any event to EventBus.
    check.execute();
    this.countDownLatch.await(10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(this.countDownLatch.getCount(), 1);
  }
}