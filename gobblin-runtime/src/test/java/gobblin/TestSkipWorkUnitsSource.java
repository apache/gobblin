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
package gobblin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.DummyExtractor;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


public class TestSkipWorkUnitsSource implements Source {
  private final String TEST_WORKUNIT_PERSISTENCE = "test.workunit.persistence";
  private final int NUMBER_OF_SKIP_WORKUNITS = 3;
  private final int NUMBER_OF_WORKUNITS = 4;

  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = new ArrayList<>();
    if (state.contains(TEST_WORKUNIT_PERSISTENCE)) {
      testSkipWorkUnitPersistence(state);
      return workUnits;
    }
    for (int i = 0; i < NUMBER_OF_WORKUNITS; i++) {
      WorkUnit workUnit = WorkUnit.createEmpty();
      if (i < NUMBER_OF_SKIP_WORKUNITS) {
        workUnit.skip();
      }
      workUnits.add(workUnit);
    }

    MultiWorkUnit mwu1 = MultiWorkUnit.createEmpty();
    MultiWorkUnit mwu2 = MultiWorkUnit.createEmpty();
    MultiWorkUnit mwu3 = MultiWorkUnit.createEmpty();
    MultiWorkUnit mwu4 = MultiWorkUnit.createEmpty();
    WorkUnit wu1 = WorkUnit.createEmpty();
    WorkUnit wu2 = WorkUnit.createEmpty();
    WorkUnit wu3 = WorkUnit.createEmpty();
    WorkUnit wu4 = WorkUnit.createEmpty();
    WorkUnit wu5 = WorkUnit.createEmpty();
    WorkUnit wu6 = WorkUnit.createEmpty();
    wu2.skip();
    // wu1 is not skipped and wu2 is skipped
    mwu1.addWorkUnit(wu1);
    mwu1.addWorkUnit(wu2);
    mwu2.addWorkUnit(wu3);
    mwu2.addWorkUnit(wu4);
    // Both wu3 and wu4 will be skipped
    mwu2.skip();

    mwu3.addWorkUnit(wu5);
    mwu3.addWorkUnit(wu6);
    mwu4.addWorkUnit(mwu3);
    // Both wu5 and wu6 will be skipped
    mwu4.skip();

    workUnits.add(mwu1);
    workUnits.add(mwu2);
    workUnits.add(mwu4);
    return workUnits;
  }

  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
    return new DummyExtractor(state);
  }

  public void testSkipWorkUnitPersistence(SourceState state) {
    if (!state.getPropAsBoolean(TEST_WORKUNIT_PERSISTENCE)) {
      return;
    }
    int skipCount = 0;
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.SKIPPED) {
        skipCount++;
      }
    }
    // Plus one for MultiWorkUnit1, plus 2 for MultiWorkUnit2, plus 2 for MultiWorkUnit4
    Assert.assertEquals(skipCount, NUMBER_OF_SKIP_WORKUNITS + 5,
        "All skipped work units are not persisted in the state store");
  }

  public void shutdown(SourceState state) {
  }
}
