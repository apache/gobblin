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
package gobblin.compliance;

import java.util.Collection;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.DataPublisher;


/**
 * The Publisher moves COMMITTED WorkUnitState to SUCCESSFUL, otherwise FAILED.
 *
 * @author adsharma
 */
public class HivePurgerPublisher extends DataPublisher {
  public HivePurgerPublisher(State state) {
    super(state);
  }

  public void initialize() {
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) {
    for (WorkUnitState state : states) {
      if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      } else {
        state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      }
    }
  }

  public void publishMetadata(Collection<? extends WorkUnitState> states) {
  }

  @Override
  public void close() {
  }
}
