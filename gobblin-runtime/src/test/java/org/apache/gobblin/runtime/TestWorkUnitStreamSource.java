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

package org.apache.gobblin.runtime;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.source.WorkUnitStreamSource;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.task.EventBusPublishingTaskFactory;
import org.apache.gobblin.writer.test.TestingEventBuses;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestWorkUnitStreamSource {

  /**
   * This test uses a slow source to verify that we can stream work units through local job launcher, with available units
   * being processes eagerly even if not all work units are available.
   */
  @Test
  public void test() throws Exception {

    String eventBusId = UUID.randomUUID().toString();
    MyListener listener = new MyListener();

    EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);
    eventBus.register(listener);

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("testStreamedSource")
        .setConfiguration(EventBusPublishingTaskFactory.EVENTBUS_ID_KEY, eventBusId)
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, MySource.class.getName())
        .setConfiguration(EventBusPublishingTaskFactory.Source.NUM_TASKS_KEY, "5");

    JobExecutionDriver driver = embeddedGobblin.runAsync();
    if (!listener.iteratorReady.tryAcquire(2, TimeUnit.SECONDS)) {
      throw new RuntimeException("Failed to get start signal.");
    }

    Assert.assertFalse(listener.tasksRun.tryAcquire(50, TimeUnit.MILLISECONDS));
    eventBus.post(new MySource.NextWorkUnit());
    Assert.assertTrue(listener.tasksRun.tryAcquire(500, TimeUnit.MILLISECONDS));
    Assert.assertFalse(listener.tasksRun.tryAcquire(50, TimeUnit.MILLISECONDS));

    eventBus.post(new MySource.NextWorkUnit());
    Assert.assertTrue(listener.tasksRun.tryAcquire(500, TimeUnit.MILLISECONDS));
    Assert.assertFalse(listener.tasksRun.tryAcquire(50, TimeUnit.MILLISECONDS));

    eventBus.post(new MySource.NextWorkUnit());
    eventBus.post(new MySource.NextWorkUnit());
    eventBus.post(new MySource.NextWorkUnit());

    JobExecutionResult result = driver.get(5, TimeUnit.SECONDS);
    Assert.assertTrue(result.isSuccessful());

    SetMultimap<String, Integer> eventsSeen = listener.getEventsSeenMap();
    Set<Integer> expected = Sets.newHashSet(0, 1, 2, 3, 4);
    Assert.assertEquals(eventsSeen.get(EventBusPublishingTaskFactory.RUN_EVENT), expected);
    Assert.assertEquals(eventsSeen.get(EventBusPublishingTaskFactory.COMMIT_EVENT), expected);
    Assert.assertEquals(eventsSeen.get(EventBusPublishingTaskFactory.PUBLISH_EVENT), expected);

  }

  public static class MyListener extends EventBusPublishingTaskFactory.EventListener {
    private Semaphore iteratorReady = new Semaphore(0);
    private Semaphore tasksRun = new Semaphore(0);

    @Subscribe
    public void iteratorReadyProcess(MySource.IteratorReady event) {
      this.iteratorReady.release();
    }

    @Subscribe
    public void taskRun(EventBusPublishingTaskFactory.Event event) {
      if (event.getType().equals(EventBusPublishingTaskFactory.RUN_EVENT)) {
        this.tasksRun.release();
      }
    }
  }

  public static class MySource extends EventBusPublishingTaskFactory.Source implements WorkUnitStreamSource<String, String> {

    @Override
    public WorkUnitStream getWorkunitStream(SourceState state) {
      int numTasks = state.getPropAsInt(NUM_TASKS_KEY);
      String eventBusId = state.getProp(EventBusPublishingTaskFactory.EVENTBUS_ID_KEY);
      EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);

      return new BasicWorkUnitStream.Builder(new WorkUnitIterator(eventBus, eventBusId, numTasks)).build();
    }

    private class WorkUnitIterator implements Iterator<WorkUnit> {
      private final Semaphore semaphore = new Semaphore(0);
      private final EventBus eventBus;
      private final String eventBusId;
      private final int maxWus;
      private int currentWus;
      private boolean promisedNext = false;

      public WorkUnitIterator(EventBus eventBus, String eventBusId, int maxWus) {
        this.eventBus = eventBus;
        this.eventBusId = eventBusId;
        this.maxWus = maxWus;
        this.currentWus = 0;
        this.eventBus.register(this);
        this.eventBus.post(new IteratorReady());
      }

      @Override
      public boolean hasNext() {
        if (this.promisedNext) {
          return true;
        }
        if (this.currentWus >= this.maxWus) {
          return false;
        }
        try {
          if (!this.semaphore.tryAcquire(5, TimeUnit.SECONDS)) {
            log.error("Failed to receive signal to emit next work unit.", new RuntimeException());
            return false;
          }
          this.promisedNext = true;
          return true;
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }

      @Override
      public WorkUnit next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        this.currentWus++;
        this.promisedNext = false;
        return MySource.this.createWorkUnit(this.currentWus - 1, this.eventBusId);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Subscribe
      public void subscribe(NextWorkUnit nwu) {
        this.semaphore.release();
      }

    }

    public static class NextWorkUnit {}

    public static class IteratorReady {}
  }

}
