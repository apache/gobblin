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

package org.apache.gobblin.service.modules.orchestration;

import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DagProcessingEngineMetricsTest {
  DagProcessingEngineMetrics dagProcessingEngineMetrics;

  @BeforeClass
  public void setup() {
    dagProcessingEngineMetrics = new DagProcessingEngineMetrics();
  }

  /*
  Checks that the dagActionsStored metric can be incremented for every DagActionType. It ensures that the default
  constructor of the DagProcessingEngine initializes a meter for each DagActionType.
   */
  @Test
  public void testMarkDagActionsStored() {
    for (DagActionStore.DagActionType dagActionType : DagActionStore.DagActionType.values()) {
      dagProcessingEngineMetrics.markDagActionsStored(dagActionType);
    }
  }

  /*
  Checks that the dagActionsObserved metric can be incremented for every DagActionType. It ensures that the default
  constructor of the DagProcessingEngine initializes a meter for each DagActionType.
   */
  @Test
  public void testMarkDagActionsObserved() {
    for (DagActionStore.DagActionType dagActionType : DagActionStore.DagActionType.values()) {
      dagProcessingEngineMetrics.markDagActionsStored(dagActionType);
    }
  }
}
