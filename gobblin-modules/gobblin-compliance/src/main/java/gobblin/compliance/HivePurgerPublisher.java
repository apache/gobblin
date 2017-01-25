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
