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

package org.apache.gobblin.metrics.event.lineage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class LineageEventTest {
  @Test
  public void testEvent() {
    final String topic = "testTopic";
    State state0 = new State();
    DatasetDescriptor source = new DatasetDescriptor("kafka", topic);
    LineageInfo.setSource(source, state0);
    DatasetDescriptor destination0 = new DatasetDescriptor("hdfs", "/data/dbchanges");
    LineageInfo.putDestination(destination0, 0, state0);
    DatasetDescriptor destination1 = new DatasetDescriptor("mysql", "kafka.testTopic");
    LineageInfo.putDestination(destination1, 1, state0);

    Map<String, LineageEventBuilder> events = LineageInfo.load(state0);
    verify(events.get("0"), topic, source, destination0, 0);
    verify(events.get("1"), topic, source, destination1, 1);

    State state1 = new State();
    LineageInfo.setSource(source, state1);
    List<State> states = Lists.newArrayList();
    states.add(state0);
    states.add(state1);

    // Test only full fledged lineage events are loaded
    try {
      Collection<LineageEventBuilder> eventsList = LineageInfo.load(states);
      Assert.assertTrue(eventsList.size() == 2);
      Assert.assertEquals(getLineageEvent(eventsList, 0), events.get("0"));
      Assert.assertEquals(getLineageEvent(eventsList, 1), events.get("1"));
    } catch (LineageException e) {
      Assert.fail("Unexpected exception");
    }

    // There are 3 full fledged lineage events
    DatasetDescriptor destination2 = new DatasetDescriptor("mysql", "kafka.testTopic2");
    LineageInfo.putDestination(destination2, 2, state1);
    try {
      Collection<LineageEventBuilder> eventsList = LineageInfo.load(states);
      Assert.assertTrue(eventsList.size() == 3);
      Assert.assertEquals(getLineageEvent(eventsList, 0), events.get("0"));
      Assert.assertEquals(getLineageEvent(eventsList, 1), events.get("1"));
      verify(getLineageEvent(eventsList, 2), topic, source, destination2, 2);
    } catch (LineageException e) {
      Assert.fail("Unexpected exception");
    }

    // Throw conflict exception when there is a conflict on a branch between 2 states
    LineageInfo.putDestination(destination2, 0, state1);
    boolean hasLineageException = false;
    try {
      Collection<LineageEventBuilder> eventsList = LineageInfo.load(states);
    } catch (LineageException e) {
      Assert.assertTrue(e instanceof LineageException.ConflictException);
      hasLineageException = true;
    }
    Assert.assertTrue(hasLineageException);
  }

  private LineageEventBuilder getLineageEvent(Collection<LineageEventBuilder> events, int branchId) {
    for (LineageEventBuilder event : events) {
      if (event.getDestination().getMetadata().get(LineageInfo.BRANCH).equals(String.valueOf(branchId))) {
        return event;
      }
    }
    return null;
  }

  private void verify(LineageEventBuilder event, String name, DatasetDescriptor source, DatasetDescriptor destination, int branchId) {
    Assert.assertEquals(event.getName(), name);
    Assert.assertEquals(event.getNamespace(), LineageEventBuilder.LIENAGE_EVENT_NAMESPACE);
    Assert.assertEquals(event.getMetadata().get(GobblinEventBuilder.EVENT_TYPE), LineageEventBuilder.LINEAGE_EVENT_TYPE);
    Assert.assertTrue(event.getSource().equals(source));

    DatasetDescriptor updatedDestination = new DatasetDescriptor(destination);
    updatedDestination.addMetadata(LineageInfo.BRANCH, String.valueOf(branchId));
    Assert.assertTrue(event.getDestination().equals(updatedDestination));

    // It only has eventType info
    Assert.assertTrue(event.getMetadata().size() == 1);
  }
}
