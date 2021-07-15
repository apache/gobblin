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

package org.apache.gobblin.destination;

import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;


public class DestinationDatasetHandlerServiceTest {

  EventSubmitter eventSubmitter = null;

  @BeforeSuite
  void setup() {
    this.eventSubmitter = mock(EventSubmitter.class);
  }

  @Test
  void testSingleHandler() throws Exception {
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.DESTINATION_DATASET_HANDLER_CLASS, TestDestinationDatasetHandler.class.getName());
    DestinationDatasetHandlerService service = new DestinationDatasetHandlerService(state, true, this.eventSubmitter);
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit wu = WorkUnit.createEmpty();
      workUnits.add(wu);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits).build();
    service.executeHandlers(workUnitStream);

    for (WorkUnit wu: workUnits) {
      Assert.assertEquals(wu.getPropAsInt(TestDestinationDatasetHandler.TEST_COUNTER_KEY), 1);
    }
  }

  @Test
  void testMultipleHandlers() throws Exception {
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.DESTINATION_DATASET_HANDLER_CLASS,
        TestDestinationDatasetHandler.class.getName() + "," + TestDestinationDatasetHandler.class.getName());
    DestinationDatasetHandlerService service = new DestinationDatasetHandlerService(state, true, this.eventSubmitter);
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit wu = WorkUnit.createEmpty();
      workUnits.add(wu);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits).build();
    service.executeHandlers(workUnitStream);

    for (WorkUnit wu: workUnits) {
      // there were 2 handlers, each should have added to counter
      Assert.assertEquals(wu.getPropAsInt(TestDestinationDatasetHandler.TEST_COUNTER_KEY), 2);
    }
  }

  @Test
  void testMultipleHandlersWhitespace() throws Exception {
    SourceState state = new SourceState();
    // add whitespace in class list
    state.setProp(ConfigurationKeys.DESTINATION_DATASET_HANDLER_CLASS,
        TestDestinationDatasetHandler.class.getName() + "    ,      " + TestDestinationDatasetHandler.class.getName());
    DestinationDatasetHandlerService service = new DestinationDatasetHandlerService(state, true, this.eventSubmitter);
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit wu = WorkUnit.createEmpty();
      workUnits.add(wu);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits).build();
    service.executeHandlers(workUnitStream);

    for (WorkUnit wu: workUnits) {
      // there were 2 handlers, each should have added to counter
      Assert.assertEquals(wu.getPropAsInt(TestDestinationDatasetHandler.TEST_COUNTER_KEY), 2);
    }
  }

  @Test
  // should not throw an exception
  void testEmpty() throws Exception {
    SourceState state = new SourceState();
    DestinationDatasetHandlerService service = new DestinationDatasetHandlerService(state, true, this.eventSubmitter);
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit wu = WorkUnit.createEmpty();
      workUnits.add(wu);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits).build();
    service.executeHandlers(workUnitStream);
  }
}
