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

package org.apache.gobblin.util;

import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Utilities for unit tests.
 */
public class TestUtils {

  /**
   * Create a {@link WorkUnitState} with a {@link org.apache.gobblin.broker.iface.SharedResourcesBroker} for running unit tests of
   * constructs.
   */
  public static WorkUnitState createTestWorkUnitState() {
    return new WorkUnitState(new WorkUnit(), new State(), SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), GobblinScopeTypes.GLOBAL.defaultScopeInstance()).
        newSubscopedBuilder(new JobScopeInstance("jobName", "testJob")));
  }

}
