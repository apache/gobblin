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

package gobblin.performance;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.util.test.FastSequentialSource;
import gobblin.writer.test.GobblinTestEventBusWriter;
import gobblin.writer.test.TestingEventBuses;


/**
 * Runs performance tests on Gobblin.
 */
public class PerformanceTest {

  public static void main(String[] args) throws Exception {
    testSourceThroughput();
    testWriterThroughput();
    testGobblinThroughput();
  }

  /**
   * Test the throughput of a Gobblin pipeline with trivial source and writers and no converters / forks, etc.
   */
  public static void testGobblinThroughput() throws Exception {
    String eventBusId = PerformanceTest.class.getName();

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("PerformanceTest")
        .setTemplate("resource:///templates/performanceTest.template")
        .setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId);

    EventHandler eventHandler = new EventHandler();
    TestingEventBuses.getEventBus(eventBusId).register(eventHandler);

    embeddedGobblin.run();

    Assert.assertEquals(eventHandler.runSummaries.size(), 1);

    GobblinTestEventBusWriter.RunSummary runSummary = eventHandler.runSummaries.get(0);

    System.out.println(String.format("Task processed %d records in %d millis, qps: %f", runSummary.getRecordsWritten(),
        runSummary.getTimeElapsedMillis(),
        (double) runSummary.getRecordsWritten() * 1000 / runSummary.getTimeElapsedMillis()));
  }

  /**
   * Test the throughput of the source used on {@link #testGobblinThroughput()} to prove it is not a bottleneck.
   */
  public static void testSourceThroughput() throws Exception {
    FastSequentialSource.FastSequentialExtractor extractor =
        new FastSequentialSource.FastSequentialExtractor(10000000, 10);

    Long thisRecord;
    long lastRecord = 0;

    long startTime = System.nanoTime();
    while (true) {
      thisRecord = extractor.readRecord(null);
      if (thisRecord == null) {
        break;
      } else {
        lastRecord = thisRecord;
      }
      // do nothing
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

    System.out.println(String.format("Source produced %d records in %d millis, qps: %f", lastRecord, elapsedMillis,
        (double) lastRecord * 1000 / elapsedMillis));
  }

  /**
   * Test the throughput of the writer used on {@link #testGobblinThroughput()} to prove it is not a bottleneck.
   */
  public static void testWriterThroughput() throws Exception {
    EventBus eventBus = new EventBus();

    EventHandler eventHandler = new EventHandler();
    eventBus.register(eventHandler);

    GobblinTestEventBusWriter writer = new GobblinTestEventBusWriter(eventBus, GobblinTestEventBusWriter.Mode.COUNTING);

    long records = 0;

    long endAt = System.currentTimeMillis() + 10000;
    long startTime = System.nanoTime();
    for (records = 0; records < 10000000 && System.currentTimeMillis() < endAt; records++) {
      writer.write(records);
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

    writer.commit();

    Assert.assertEquals(eventHandler.runSummaries.get(0).getRecordsWritten(), records);

    System.out.println(String.format("Writer consumed %d records in %d millis, qps: %f", records, elapsedMillis,
        (double) records * 1000 / elapsedMillis));

  }

  private static class EventHandler {
    List<GobblinTestEventBusWriter.RunSummary> runSummaries = Lists.newArrayList();

    @Subscribe
    public void registerCount(TestingEventBuses.Event event) {
      this.runSummaries.add((GobblinTestEventBusWriter.RunSummary) event.getValue());
    }
  }

}
