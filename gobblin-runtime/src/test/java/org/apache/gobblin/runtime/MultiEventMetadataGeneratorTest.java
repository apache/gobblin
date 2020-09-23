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

import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.metrics.event.EventName;
import org.apache.gobblin.runtime.api.EventMetadataGenerator;
import org.apache.gobblin.runtime.api.MultiEventMetadataGenerator;


public class MultiEventMetadataGeneratorTest {

  @Test
  public void testInstantiate() {
    JobContext jobContext = Mockito.mock(JobContext.class);
    MultiEventMetadataGenerator multiEventMetadataGenerator = new MultiEventMetadataGenerator(ImmutableList.of(
        "org.apache.gobblin.runtime.MultiEventMetadataGeneratorTest$DummyEventMetadataGenerator",
        "org.apache.gobblin.runtime.MultiEventMetadataGeneratorTest$DummyEventMetadataGenerator2"));

    Map<String, String> metadata = multiEventMetadataGenerator.getMetadata(jobContext, EventName.getEnumFromEventId("JobCompleteTimer"));
    Assert.assertEquals(metadata.size(), 3);
    Assert.assertEquals(metadata.get("dummyKey11"), "dummyValue11");
    Assert.assertEquals(metadata.get("dummyKey12"), "dummyValue22");
    Assert.assertEquals(metadata.get("dummyKey21"), "dummyValue21");
  }

  public static class DummyEventMetadataGenerator implements EventMetadataGenerator {

    @Override
    public Map<String, String> getMetadata(JobContext jobContext, EventName eventName) {
      return ImmutableMap.of("dummyKey11", "dummyValue11", "dummyKey12", "dummyValue12");
    }
  }

  public static class DummyEventMetadataGenerator2 implements EventMetadataGenerator {

    @Override
    public Map<String, String> getMetadata(JobContext jobContext, EventName eventName) {
      return ImmutableMap.of("dummyKey21", "dummyValue21", "dummyKey12", "dummyValue22");
    }
  }
}