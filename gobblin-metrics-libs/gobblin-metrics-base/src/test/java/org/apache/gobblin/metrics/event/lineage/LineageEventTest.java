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

import java.util.Map;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import org.testng.Assert;
import org.testng.annotations.Test;


public class LineageEventTest {
  @Test
  public void testEvent() {
    final String topic = "testTopic";
    State state = new State();
    LineageInfo.register(topic, LineageEventBuilder.LineageType.TRANSFORMED, state);
    DatasetDescriptor source = new DatasetDescriptor("kafka", topic);
    LineageInfo.putSource(source, state);

    DatasetDescriptor destination0 = new DatasetDescriptor("hdfs", "/data/dbchanges");
    LineageInfo.putDestination(destination0, 0, state);
    DatasetDescriptor destination1 = new DatasetDescriptor("mysql", "kafka.testTopic");
    LineageInfo.putDestination(destination1, 1, state);

    Map<String, LineageEventBuilder> events = LineageInfo.load(state);
    verify(events.get("0"), topic, source, destination0, 0);
    verify(events.get("1"), topic, source, destination1, 1);
  }

  void verify(LineageEventBuilder event, String name, DatasetDescriptor source, DatasetDescriptor destination, int branchId) {
    Assert.assertEquals(event.getName(), name);
    Assert.assertEquals(event.getNamespace(), GobblinEventBuilder.DEFAULT_NAMESPACE);
    Assert.assertEquals(event.getMetadata().get(GobblinEventBuilder.EVENT_TYPE), LineageEventBuilder.LINEAGE_EVENT_TYPE);
    Assert.assertTrue(event.getSource().equals(source));

    DatasetDescriptor updatedDestination = new DatasetDescriptor(destination);
    updatedDestination.addMetadata(LineageInfo.BRANCH, String.valueOf(branchId));
    Assert.assertTrue(event.getDestination().equals(updatedDestination));

    // It only has eventType info
    Assert.assertTrue(event.getMetadata().size() == 1);
  }
}
