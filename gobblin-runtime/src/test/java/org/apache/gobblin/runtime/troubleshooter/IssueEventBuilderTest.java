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

package org.apache.gobblin.runtime.troubleshooter;

import java.time.ZonedDateTime;
import java.util.HashMap;

import org.testng.annotations.Test;

import org.apache.gobblin.metrics.GobblinTrackingEvent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class IssueEventBuilderTest {

  @Test
  public void canSerializeIssue() {

    IssueEventBuilder eventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);

    HashMap<String, String> testProperties = new HashMap<String, String>() {{
      put("testKey", "test value %'\"");
    }};

    Issue issue =
        Issue.builder().summary("test summary").details("test details").time(ZonedDateTime.now()).code("Code1")
            .severity(IssueSeverity.ERROR).exceptionClass("com.TestException").properties(testProperties).build();

    eventBuilder.setIssue(issue);

    GobblinTrackingEvent trackingEvent = eventBuilder.build();

    assertTrue(IssueEventBuilder.isIssueEvent(trackingEvent));

    IssueEventBuilder deserializedEventBuilder = IssueEventBuilder.fromEvent(trackingEvent);
    assertNotNull(deserializedEventBuilder);
    assertEquals("Code1", deserializedEventBuilder.getIssue().getCode());

    assertEquals("Code1", IssueEventBuilder.getIssueFromEvent(trackingEvent).getCode());
  }
}