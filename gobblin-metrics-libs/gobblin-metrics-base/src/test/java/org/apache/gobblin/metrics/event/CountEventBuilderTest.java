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

package org.apache.gobblin.metrics.event;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.notification.EventNotification;
import org.apache.gobblin.metrics.notification.Notification;


public class CountEventBuilderTest {

  @Test
  public void test() {
    String name = "TestName";
    int count = 10;
    MetricContext context = new MetricContext.Builder("name").build();
    CountEventBuilder countEventBuilder = new CountEventBuilder(name, count);
    context.addNotificationTarget(new com.google.common.base.Function<Notification, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable Notification input) {
        if (input instanceof EventNotification) {
          GobblinTrackingEvent event = ((EventNotification) input).getEvent();
          Map<String, String> metadata = event.getMetadata();
          Assert.assertEquals(metadata.containsKey(GobblinEventBuilder.EVENT_TYPE), true);
          Assert.assertEquals(metadata.containsKey(CountEventBuilder.COUNT_KEY), true);
          Assert.assertEquals(metadata.get(GobblinEventBuilder.EVENT_TYPE), CountEventBuilder.COUNT_EVENT_TYPE);
          Assert.assertEquals(event.getName(), name);
          Assert.assertEquals(event.getNamespace(), GobblinEventBuilder.NAMESPACE);
          Assert.assertEquals(Integer.parseInt(metadata.get(CountEventBuilder.COUNT_KEY)), count);
        }
        return null;
      }
    });
    EventSubmitter.submit(context, countEventBuilder);
  }

  @Test
  public void fromEventTest() {
    String name = "TestName";
    int count = 10;
    CountEventBuilder countEventBuilder = new CountEventBuilder(name, count);
    GobblinTrackingEvent event = countEventBuilder.build();

    //Count Event
    CountEventBuilder builderFromEvent = CountEventBuilder.fromEvent(event);
    Assert.assertEquals(CountEventBuilder.isCountEvent(event), true);
    Assert.assertNotNull(builderFromEvent);
    Assert.assertEquals(builderFromEvent.getName(), name);
    Assert.assertEquals(builderFromEvent.getCount(), count);

    // General Event
    event = new GobblinTrackingEvent();
    countEventBuilder = CountEventBuilder.fromEvent(event);
    Assert.assertEquals(CountEventBuilder.isCountEvent(event), false);
    Assert.assertEquals(countEventBuilder, null);
  }

}
