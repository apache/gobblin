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

package org.apache.gobblin.source.extractor.extract;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.configuration.WorkUnitState.WorkingState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;


@Test(groups = { "gobblin.source.extractor.extract" })
public class AbstractSourceTest {

  private TestSource<String, String> testSource;
  private List<WorkUnitState> previousWorkUnitStates;
  private List<WorkUnitState> expectedPreviousWorkUnitStates;

  @BeforeClass
  public void setUpBeforeClass() {
    this.testSource = new TestSource<>();

    WorkUnitState committedWorkUnitState = new WorkUnitState();
    committedWorkUnitState.setWorkingState(WorkingState.COMMITTED);
    WorkUnitState successfulWorkUnitState = new WorkUnitState();
    successfulWorkUnitState.setWorkingState(WorkingState.SUCCESSFUL);
    WorkUnitState failedWorkUnitState = new WorkUnitState();
    failedWorkUnitState.setWorkingState(WorkingState.FAILED);

    this.previousWorkUnitStates =
        Lists.newArrayList(committedWorkUnitState, successfulWorkUnitState, failedWorkUnitState);
    this.expectedPreviousWorkUnitStates = Lists.newArrayList(successfulWorkUnitState, failedWorkUnitState);

  }

  /**
   * Test the never-retry policy.
   */
  @Test
  public void testGetPreviousWorkUnitStatesNeverRetry() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "never");

    Assert.assertEquals(this.testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry disabled.
   */
  @Test
  public void testGetPreviousWorkUnitStatesDisabledRetry() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.FALSE);

    Assert.assertEquals(this.testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on partial, but the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryFullCommit() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onpartial");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "full");

    Assert.assertEquals(this.testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on full, but the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryPartialCommit() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onfull");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");

    Assert.assertEquals(this.testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on full, and the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryFullCommit() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onfull");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "full");

    List<WorkUnitState> returnedWorkUnitStates = this.testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates, this.expectedPreviousWorkUnitStates);
  }

  /**
   * Test when work unit retry policy is on partial, and the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryPartialCommit() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onpartial");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");

    List<WorkUnitState> returnedWorkUnitStates = this.testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates, this.expectedPreviousWorkUnitStates);
  }

  /**
   * Test the always-retry policy, with WORK_UNIT_RETRY_ENABLED_KEY enabled.
   */
  @Test
  public void testGetPreviousWorkUnitStatesEnabledRetry() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.TRUE);

    List<WorkUnitState> returnedWorkUnitStates = this.testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates, this.expectedPreviousWorkUnitStates);
  }

  /**
   * Test under always-retry policy, the overwrite_configs_in_statestore enabled.
   * The previous workUnitState should be reset with the config in the current source.
   */
  @Test
  public void testGetPreviousWorkUnitStatesWithConfigOverWrittenEnabled() {
    for (WorkUnitState workUnitState : this.previousWorkUnitStates) {
      workUnitState.setProp("a", "3");
      workUnitState.setProp("b", "4");
    }
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "always");
    sourceState.setProp(ConfigurationKeys.OVERWRITE_CONFIGS_IN_STATESTORE, Boolean.TRUE);

    // random properties for test
    sourceState.setProp("a", "1");
    sourceState.setProp("b", "2");

    List<WorkUnitState> returnedWorkUnitStates = this.testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    for (WorkUnitState workUnitState : returnedWorkUnitStates) {
      Assert.assertEquals(workUnitState.getProp("a"), "1");
      Assert.assertEquals(workUnitState.getProp("b"), "2");
    }
  }

  /**
   * Test under always-retry policy, the overwrite_configs_in_statestore disabled (default).
   * The previous workUnitState would not be reset with the config in the current source.
   */
  @Test
  public void testGetPreviousWorkUnitStatesWithConfigOverWrittenDisabled() {
    SourceState sourceState = new SourceState(new State(), this.previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "always");

    // random properties for test
    sourceState.setProp("a", "1");
    sourceState.setProp("b", "2");

    List<WorkUnitState> returnedWorkUnitStates = this.testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates, this.expectedPreviousWorkUnitStates);

    for (WorkUnitState workUnitState : returnedWorkUnitStates) {
      Assert.assertEquals(workUnitState.contains("a"), false);
      Assert.assertEquals(workUnitState.contains("b"), false);
    }
  }

  // Class for test AbstractSource
  public class TestSource<S, D> extends AbstractSource<S, D> {

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      return null;
    }

    @Override
    public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
      return null;
    }

    @Override
    public void shutdown(SourceState state) {
    }

  }

}
