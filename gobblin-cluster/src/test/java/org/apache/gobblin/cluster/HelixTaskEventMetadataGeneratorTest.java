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
package org.apache.gobblin.cluster;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.api.TaskEventMetadataGenerator;
import org.apache.gobblin.util.TaskEventMetadataUtils;

@Test
public class HelixTaskEventMetadataGeneratorTest {

  public void testGetMetadata() {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_EVENT_METADATA_GENERATOR_CLASS_KEY, "helixtask");
    state.setProp(GobblinClusterConfigurationKeys.CONTAINER_ID_KEY, "container-1");
    state.setProp(GobblinClusterConfigurationKeys.HELIX_TASK_ID_KEY, "task-1");
    state.setProp(GobblinClusterConfigurationKeys.HELIX_JOB_ID_KEY, "job-1");

    TaskEventMetadataGenerator metadataGenerator = TaskEventMetadataUtils.getTaskEventMetadataGenerator(state);
    //Ensure instantiation is done correctly
    Assert.assertTrue(metadataGenerator != null);

    //Ensure metadata map is correctly populated
    Map<String, String> metadataMap = metadataGenerator.getMetadata(state, "testEventName");
    Assert.assertEquals(metadataMap.size(), 5);
    Assert.assertEquals(metadataMap.get(HelixTaskEventMetadataGenerator.HELIX_INSTANCE_KEY), "");
    Assert.assertEquals(metadataMap.get(HelixTaskEventMetadataGenerator.CONTAINER_ID_KEY), "container-1");
    Assert.assertEquals(metadataMap.get(HelixTaskEventMetadataGenerator.HOST_NAME_KEY), "");
    Assert.assertEquals(metadataMap.get(HelixTaskEventMetadataGenerator.HELIX_TASK_ID_KEY), "task-1");
    Assert.assertEquals(metadataMap.get(HelixTaskEventMetadataGenerator.HELIX_JOB_ID_KEY), "job-1");
  }
}