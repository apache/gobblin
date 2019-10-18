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

package org.apache.gobblin.azure.adf;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Properties;

@Test
public class ADFPipelineSourceTest {
  @Test
  public void testGetWorkunits() {
    Properties prop = new Properties();
    prop.put("k1", "v1");
    State state = new State(prop);
    SourceState sourceState = new SourceState(state);
    List<WorkUnit> workunits = new ADFPipelineSource().getWorkunits(sourceState);

    Assert.assertEquals(workunits.size(), 1);
    Assert.assertEquals(workunits.get(0).getProp("k1"), "v1");
  }
}
