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

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * Test all {@link GobblinEventBuilder}s
 */
public class GobblinEventTest {

  @Test
  public void testJobStateEvent() {
    // Test build JobStateEvent
    /// build without status
    String jobUrl = "jobUrl";
    JobStateEventBuilder eventBuilder = new JobStateEventBuilder(JobStateEventBuilder.MRJobState.MR_JOB_STATE);
    eventBuilder.jobTrackingURL = jobUrl;
    GobblinTrackingEvent event = eventBuilder.build();
    Assert.assertEquals(event.getName(), "MRJobState");
    Assert.assertNull(event.getNamespace());
    Map<String, String> metadata = event.getMetadata();
    Assert.assertEquals(metadata.size(), 2);
    Assert.assertEquals(metadata.get("eventType"), "JobStateEvent");
    Assert.assertEquals(metadata.get("jobTrackingURL"), jobUrl);
    Assert.assertNull(metadata.get("jobState"));

    /// build with status
    eventBuilder.status = JobStateEventBuilder.Status.FAILED;
    event = eventBuilder.build();
    metadata = event.getMetadata();
    Assert.assertEquals(metadata.size(), 3);
    Assert.assertEquals(metadata.get("jobState"), "FAILED");

    // Test parse from GobblinTrackingEvent
    JobStateEventBuilder parsedEvent = JobStateEventBuilder.fromEvent(event);
    Assert.assertEquals(parsedEvent.status, JobStateEventBuilder.Status.FAILED);
    Assert.assertEquals(parsedEvent.jobTrackingURL, jobUrl);
    Assert.assertEquals(parsedEvent.getMetadata().size(), 1);
  }

  @Test
  public void testEntityMissingEvent() {
    // Test build EntityMissingEvent
    String instance = "mytopic";
    String eventClass = "TopicMissing";
    EntityMissingEventBuilder eventBuilder = new EntityMissingEventBuilder(eventClass, instance);
    GobblinTrackingEvent event = eventBuilder.build();
    Assert.assertEquals(event.getName(), eventClass);
    Assert.assertNull(event.getNamespace());
    Map<String, String> metadata = event.getMetadata();
    Assert.assertEquals(metadata.size(), 2);
    Assert.assertEquals(metadata.get("eventType"), "EntityMissingEvent");
    Assert.assertEquals(metadata.get("entityInstance"), instance);

    // Test parse from GobblinTrackingEvent
    Assert.assertNull(JobStateEventBuilder.fromEvent(event));
    EntityMissingEventBuilder parsedEvent = EntityMissingEventBuilder.fromEvent(event);
    Assert.assertEquals(parsedEvent.getName(), eventClass);
    Assert.assertEquals(parsedEvent.getInstance(), instance);
    Assert.assertEquals(parsedEvent.getMetadata().size(), 1);
  }
}
