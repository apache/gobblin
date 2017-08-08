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

package org.apache.gobblin.task;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.writer.test.TestingEventBuses;
import java.io.File;
import java.util.Set;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CustomTaskTest {

  @Test
  public void testCustomTask() throws Exception {

    String eventBusId = UUID.randomUUID().toString();
    EventBusPublishingTaskFactory.EventListener listener = new EventBusPublishingTaskFactory.EventListener();

    EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);
    eventBus.register(listener);

    JobExecutionResult result =
        new EmbeddedGobblin("testJob").setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, EventBusPublishingTaskFactory.Source.class.getName())
        .setConfiguration(EventBusPublishingTaskFactory.Source.NUM_TASKS_KEY, "10").setConfiguration(EventBusPublishingTaskFactory.EVENTBUS_ID_KEY, eventBusId)
        .run();

    Assert.assertTrue(result.isSuccessful());

    SetMultimap<String, Integer> seenEvents = HashMultimap.create();
    Set<Integer> expected = Sets.newHashSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);


    for (EventBusPublishingTaskFactory.Event event : listener.getEvents()) {
      seenEvents.put(event.getType(), event.getId());
    }

    Assert.assertEquals(seenEvents.get("run"), expected);
    Assert.assertEquals(seenEvents.get("commit"), expected);
    Assert.assertEquals(seenEvents.get("publish"), expected);
  }

  @Test
  public void testStatePersistence() throws Exception {
    File stateStore = Files.createTempDir();
    stateStore.deleteOnExit();

    String eventBusId = UUID.randomUUID().toString();
    EventBusPublishingTaskFactory.EventListener listener = new EventBusPublishingTaskFactory.EventListener();

    EventBus eventBus = TestingEventBuses.getEventBus(eventBusId);
    eventBus.register(listener);

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("testJob")
        .setConfiguration(EventBusPublishingTaskFactory.EVENTBUS_ID_KEY, eventBusId)
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, EventBusPublishingTaskFactory.Source.class.getName())
        .setConfiguration(EventBusPublishingTaskFactory.Source.NUM_TASKS_KEY, "2")
        .setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, Boolean.toString(true))
        .setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStore.getAbsolutePath());

    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());

    SetMultimap<String, Integer> seenEvents = HashMultimap.create();
    for (EventBusPublishingTaskFactory.Event event : listener.getEvents()) {
      seenEvents.put(event.getType(), event.getId());
    }
    Assert.assertEquals(seenEvents.get("previousState").size(), 0);

    result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());

    seenEvents = HashMultimap.create();
    for (EventBusPublishingTaskFactory.Event event : listener.getEvents()) {
      seenEvents.put(event.getType(), event.getId());
    }

    Assert.assertEquals(seenEvents.get("previousState"), Sets.newHashSet(0, 1));

  }
}
