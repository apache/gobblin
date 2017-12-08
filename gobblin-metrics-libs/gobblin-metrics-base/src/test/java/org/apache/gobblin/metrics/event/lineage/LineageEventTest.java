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
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


/**
 * Test for loading linage events from state
 */
public class LineageEventTest {
  @Test
  public void testEvent() {
    final String topic = "testTopic";
    final String kafka = "kafka";
    final String hdfs = "hdfs";
    final String mysql = "mysql";
    final String branch = "branch";

    State state0 = new State();
    DatasetDescriptor source = new DatasetDescriptor(kafka, topic);
    LineageInfo.setSource(source, state0);
    DatasetDescriptor destination00 = new DatasetDescriptor(hdfs, "/data/dbchanges");
    destination00.addMetadata(branch, "0");
    LineageInfo.putDestination(destination00, 0, state0);
    DatasetDescriptor destination01 = new DatasetDescriptor(mysql, "kafka.testTopic");
    destination01.addMetadata(branch, "1");
    LineageInfo.putDestination(destination01, 1, state0);

    Map<String, LineageEventBuilder> events = LineageInfo.load(state0);
    verify(events.get("0"), topic, source, destination00);
    verify(events.get("1"), topic, source, destination01);

    State state1 = new State();
    LineageInfo.setSource(source, state1);
    List<State> states = Lists.newArrayList();
    states.add(state0);
    states.add(state1);

    // Test only full fledged lineage events are loaded
    Collection<LineageEventBuilder> eventsList = LineageInfo.load(states);
    Assert.assertTrue(eventsList.size() == 2);
    Assert.assertEquals(getLineageEvent(eventsList, 0, hdfs), events.get("0"));
    Assert.assertEquals(getLineageEvent(eventsList, 1, mysql), events.get("1"));

    // There are 3 full fledged lineage events
    DatasetDescriptor destination12 = new DatasetDescriptor(mysql, "kafka.testTopic2");
    destination12.addMetadata(branch, "2");
    LineageInfo.putDestination(destination12, 2, state1);
    eventsList = LineageInfo.load(states);
    Assert.assertTrue(eventsList.size() == 3);
    Assert.assertEquals(getLineageEvent(eventsList, 0, hdfs), events.get("0"));
    Assert.assertEquals(getLineageEvent(eventsList, 1, mysql), events.get("1"));
    verify(getLineageEvent(eventsList, 2, mysql), topic, source, destination12);


    // There 5 lineage events put, but only 4 unique lineage events
    DatasetDescriptor destination10 = destination12;
    LineageInfo.putDestination(destination10, 0, state1);
    DatasetDescriptor destination11 = new DatasetDescriptor("hive", "kafka.testTopic1");
    destination11.addMetadata(branch, "1");
    LineageInfo.putDestination(destination11, 1, state1);
    eventsList = LineageInfo.load(states);
    Assert.assertTrue(eventsList.size() == 4);
    Assert.assertEquals(getLineageEvent(eventsList, 0, hdfs), events.get("0"));
    Assert.assertEquals(getLineageEvent(eventsList, 1, mysql), events.get("1"));
    // Either branch 0 or 2 of state 1 is selected
    LineageEventBuilder event12 = getLineageEvent(eventsList, 0, mysql);
    if (event12 == null) {
      event12 = getLineageEvent(eventsList, 2, mysql);
    }
    verify(event12, topic, source, destination12);
    verify(getLineageEvent(eventsList, 1, "hive"), topic, source, destination11);
  }

  private LineageEventBuilder getLineageEvent(Collection<LineageEventBuilder> events, int branchId, String destinationPlatform) {
    for (LineageEventBuilder event : events) {
      if (event.getDestination().getPlatform().equals(destinationPlatform) &&
          event.getDestination().getMetadata().get(DatasetConstants.BRANCH).equals(String.valueOf(branchId))) {
        return event;
      }
    }
    return null;
  }

  private void verify(LineageEventBuilder event, String name, DatasetDescriptor source, DatasetDescriptor destination) {
    Assert.assertEquals(event.getName(), name);
    Assert.assertEquals(event.getNamespace(), LineageEventBuilder.LIENAGE_EVENT_NAMESPACE);
    Assert.assertEquals(event.getMetadata().get(GobblinEventBuilder.EVENT_TYPE), LineageEventBuilder.LINEAGE_EVENT_TYPE);
    Assert.assertTrue(event.getSource().equals(source));
    Assert.assertTrue(event.getDestination().equals(destination));
  }
}
