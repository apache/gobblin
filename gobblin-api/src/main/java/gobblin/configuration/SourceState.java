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
package gobblin.configuration;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/***
 * Shim layer for org.apache.gobblin.configuration.SourceState
 */
public class SourceState extends org.apache.gobblin.configuration.SourceState {
  /**
   * Default constructor.
   */
  public SourceState() {
    super();
  }

  public SourceState(State properties, Iterable<WorkUnitState> prevWorkUnitStates) {
    super(properties, adaptWorkUnitStates(prevWorkUnitStates));
  }

  public SourceState(State properties, Map<String, ? extends SourceState> previousDatasetStatesByUrns,
      Iterable<WorkUnitState> previousWorkUnitStates) {
    super(properties, previousDatasetStatesByUrns, adaptWorkUnitStates(previousWorkUnitStates));
  }

  private static Iterable<org.apache.gobblin.configuration.WorkUnitState> adaptWorkUnitStates(Iterable<WorkUnitState> prevWorkUnitStates) {
    return Iterables.transform(prevWorkUnitStates, new Function<WorkUnitState, org.apache.gobblin.configuration.WorkUnitState>() {
      @Override
      public org.apache.gobblin.configuration.WorkUnitState apply(WorkUnitState input) {
        return input;
      }
    });
  }
}
