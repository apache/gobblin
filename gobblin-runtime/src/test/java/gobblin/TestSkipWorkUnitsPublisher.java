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
import java.util.Collection;

import org.testng.Assert;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.DataPublisher;


/**
 * Created by adsharma on 11/22/16.
 */
public class TestSkipWorkUnitsPublisher extends DataPublisher {
  public TestSkipWorkUnitsPublisher(State state)
      throws IOException {
    super(state);
  }

  public void initialize() {
  }

  public void publishData(Collection<? extends WorkUnitState> states)
      throws IOException {
    for (WorkUnitState state : states) {
      Assert.assertTrue(state.getWorkingState() != WorkUnitState.WorkingState.SKIPPED,
          "Skipped WorkUnit shouldn't be passed to publisher");
      if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      } else {
        state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      }
    }
  }

  public void publishMetadata(Collection<? extends WorkUnitState> states) {
  }

  public void close() {
  }
}
