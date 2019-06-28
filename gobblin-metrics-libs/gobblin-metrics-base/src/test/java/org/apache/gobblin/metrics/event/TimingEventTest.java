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


public class TimingEventTest {

  @Test
  public void test() {
    String name = "TestName";
    String namepace = "TestNamespace";
    MetricContext context = new MetricContext.Builder("name").build();
    context.addNotificationTarget(new com.google.common.base.Function<Notification, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable Notification input) {
        if (input instanceof EventNotification) {
          GobblinTrackingEvent event = ((EventNotification) input).getEvent();
          Map<String, String> metadata = event.getMetadata();
          Assert.assertEquals(event.getNamespace(), namepace);
          Assert.assertEquals(metadata.containsKey(GobblinEventBuilder.EVENT_TYPE), true);
          Assert.assertEquals(metadata.containsKey(TimingEvent.METADATA_START_TIME), true);
          Assert.assertEquals(metadata.containsKey(TimingEvent.METADATA_END_TIME), true);
          Assert.assertEquals(metadata.containsKey(TimingEvent.METADATA_DURATION), true);
          Assert.assertEquals(metadata.get(GobblinEventBuilder.EVENT_TYPE), TimingEvent.METADATA_TIMING_EVENT);
          Assert.assertEquals(event.getName(), name);
        }
        return null;
      }
    });
    TimingEvent timingEvent = new TimingEvent(new EventSubmitter.Builder(context, namepace).build(), name);
    timingEvent.close();
  }

  @Test
  public void fromEventTest() {
    String name = "TestName";
    String namepace = "TestNamespace";
    MetricContext context = new MetricContext.Builder("name").build();
    TimingEvent timingEventBuilder = new TimingEvent(new EventSubmitter.Builder(context, namepace).build(), name);
    GobblinTrackingEvent event = timingEventBuilder.build();
    timingEventBuilder.close();

    //Timing Event
    TimingEvent builderFromEvent = TimingEvent.fromEvent(event);
    Assert.assertEquals(TimingEvent.isTimingEvent(event), true);
    Assert.assertNotNull(builderFromEvent);
    Assert.assertEquals(builderFromEvent.getName(), name);
    Assert.assertTrue(builderFromEvent.getStartTime() <= System.currentTimeMillis());
    Assert.assertTrue(builderFromEvent.getEndTime() >= builderFromEvent.getStartTime());
    Assert.assertTrue(builderFromEvent.getDuration() >= 0);

    // General Event
    event = new GobblinTrackingEvent();
    timingEventBuilder = TimingEvent.fromEvent(event);
    Assert.assertEquals(TimingEvent.isTimingEvent(event), false);
    Assert.assertEquals(timingEventBuilder, null);
  }
}
