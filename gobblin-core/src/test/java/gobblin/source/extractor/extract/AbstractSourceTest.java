package gobblin.source.extractor.extract;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.source.extractor.Extractor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AbstractSourceTest {

  WorkUnitState committedWorkUnitState;
  WorkUnitState successfulWorkUnitState;
  WorkUnitState failedWorkUnitState;
  List<WorkUnitState> previousWorkUnitStates;

  @BeforeClass
  public void setUpBeforeClass() {
    WorkUnitState committedWorkUnitState = new WorkUnitState();
    committedWorkUnitState.setWorkingState(WorkingState.COMMITTED);
    WorkUnitState successfulWorkUnitState = new WorkUnitState();
    successfulWorkUnitState.setWorkingState(WorkingState.SUCCESSFUL);
    WorkUnitState failedWorkUnitState = new WorkUnitState();
    failedWorkUnitState.setWorkingState(WorkingState.FAILED);
    previousWorkUnitStates = Lists.newArrayList();
    previousWorkUnitStates.add(committedWorkUnitState);
    previousWorkUnitStates.add(successfulWorkUnitState);
    previousWorkUnitStates.add(failedWorkUnitState);
  }

  /**
   * Test the never-retry policy.
   */
  @Test
  public void testGetPreviousWorkUnitStatesNeverRetry() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "never");

    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry disabled.
   */
  @Test
  public void testGetPreviousWorkUnitStatesDisabledRetry() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.FALSE);

    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on partial, but the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryFullCommit() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onpartial");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "full");

    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on full, but the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryPartialCommit() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onfull");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");

    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(sourceState), Collections.EMPTY_LIST);
  }

  /**
   * Test when work unit retry policy is on full, and the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryFullCommit() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onfull");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "full");

    List<WorkUnitState> returnedWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates.size(), 2);
    Assert.assertEquals(
        (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.SUCCESSFUL) && returnedWorkUnitStates
            .get(1).getWorkingState().equals(WorkingState.FAILED))
            || (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.FAILED) && returnedWorkUnitStates
                .get(1).getWorkingState().equals(WorkingState.SUCCESSFUL)), true);
  }

  /**
   * Test when work unit retry policy is on partial, and the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryPartialCommit() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "onpartial");
    sourceState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");

    List<WorkUnitState> returnedWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates.size(), 2);
    Assert.assertEquals(
        (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.SUCCESSFUL) && returnedWorkUnitStates
            .get(1).getWorkingState().equals(WorkingState.FAILED))
            || (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.FAILED) && returnedWorkUnitStates
                .get(1).getWorkingState().equals(WorkingState.SUCCESSFUL)), true);
  }

  /**
   * Test the always-retry policy, with WORK_UNIT_RETRY_ENABLED_KEY enabled.
   */
  @Test
  public void testGetPreviousWorkUnitStatesEabledRetry() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.TRUE);

    List<WorkUnitState> returnedWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(sourceState);

    Assert.assertEquals(returnedWorkUnitStates.size(), 2);
    Assert.assertEquals(
        (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.SUCCESSFUL) && returnedWorkUnitStates
            .get(1).getWorkingState().equals(WorkingState.FAILED))
            || (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.FAILED) && returnedWorkUnitStates
                .get(1).getWorkingState().equals(WorkingState.SUCCESSFUL)), true);
  }

  /**
   * Test under always-retry policy, the overrite_configs_in_statestore enabled.
   * The previous workUnitState should be reset with the config in the current source.
   */
  @Test
  public void testGetPreviousWorkUnitStatesWithConfigOverWrittenEnabled() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "always");
    sourceState.setProp(ConfigurationKeys.OVERWRITE_CONFIGS_IN_STATESTORE, Boolean.TRUE);

    // random properties for test
    sourceState.setProp("a", "1");
    sourceState.setProp("b", "2");

    List<WorkUnitState> returnedWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(sourceState);
    Assert.assertEquals(returnedWorkUnitStates.size(), 2);
    Assert.assertEquals(
        (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.SUCCESSFUL) && returnedWorkUnitStates
            .get(1).getWorkingState().equals(WorkingState.FAILED))
            || (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.FAILED) && returnedWorkUnitStates
                .get(1).getWorkingState().equals(WorkingState.SUCCESSFUL)), true);

    for (WorkUnitState workUnitState : returnedWorkUnitStates) {
      Assert.assertEquals(workUnitState.getProp("a"), "1");
      Assert.assertEquals(workUnitState.getProp("b"), "2");
    }
  }

  /**
   * Test under always-retry policy, the overrite_configs_in_statestore disabled (default).
   * The previous workUnitState would not be reset with the config in the current source.
   */
  @Test
  public void testGetPreviousWorkUnitStatesWithConfigOverWrittenDisabled() {
    TestSource testSource = new TestSource();

    SourceState sourceState = new SourceState(new State(), previousWorkUnitStates);
    sourceState.setProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY, "always");

    // random properties for test
    sourceState.setProp("a", "1");
    sourceState.setProp("b", "2");

    List<WorkUnitState> returnedWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(sourceState);
    Assert.assertEquals(returnedWorkUnitStates.size(), 2);
    Assert.assertEquals(
        (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.SUCCESSFUL) && returnedWorkUnitStates
            .get(1).getWorkingState().equals(WorkingState.FAILED))
            || (returnedWorkUnitStates.get(0).getWorkingState().equals(WorkingState.FAILED) && returnedWorkUnitStates
                .get(1).getWorkingState().equals(WorkingState.SUCCESSFUL)), true);

    for (WorkUnitState workUnitState : returnedWorkUnitStates) {
      Assert.assertEquals(workUnitState.contains("a"), false);
      Assert.assertEquals(workUnitState.contains("b"), false);
    }
  }

  // Class for test AbstractSource
  public class TestSource extends AbstractSource {

    @Override
    public List getWorkunits(SourceState state) {
      return null;
    }

    @Override
    public Extractor getExtractor(WorkUnitState state) throws IOException {
      return null;
    }

    @Override
    public void shutdown(SourceState state) {
    }

  }

}
