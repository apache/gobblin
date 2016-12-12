/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.extractor.EmptyExtractor;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;


/**
 * Created by adsharma on 11/22/16.
 */
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
    return workUnits;
  }

  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
    return new EmptyExtractor(null);
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
    Assert.assertEquals(skipCount, NUMBER_OF_SKIP_WORKUNITS,
        "All skipped work units are not persisted in the state store");
  }

  public void shutdown(SourceState state) {
  }
}
