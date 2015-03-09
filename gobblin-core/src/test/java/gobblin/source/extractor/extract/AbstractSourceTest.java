package gobblin.source.extractor.extract;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class AbstractSourceTest {
  
  /**
   * Test the never-retry policy.
   */
  @Test
  public void testGetPreviousWorkUnitStatesNeverRetry() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(mockedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("never");
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), Collections.EMPTY_LIST);
  }
  
  /**
   * Test when work unit retry disabled.
   */
  @Test
  public void testGetPreviousWorkUnitStatesDisabledRetry() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.FALSE);
    when(mockedState.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.TRUE)).thenReturn(Boolean.FALSE);
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), Collections.EMPTY_LIST);
  }
  
  /**
   * Test when work unit retry policy is on partial, but the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryFullCommit() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    when(mockedState.getPreviousWorkUnitStates()).thenReturn(Collections.singletonList(new WorkUnitState()));
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(mockedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("onpartial");
    when(mockedState.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY)).thenReturn("full");
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), Collections.EMPTY_LIST);
  }
  
  /**
   * Test when work unit retry policy is on full, but the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryPartialCommit() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    when(mockedState.getPreviousWorkUnitStates()).thenReturn(Collections.singletonList(new WorkUnitState()));
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(mockedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("onfull");
    when(mockedState.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY)).thenReturn("partial");
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), Collections.EMPTY_LIST);
  }
  
  /**
   * Test when work unit retry policy is on full, and the job commit policy is "full".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnFullRetryFullCommit() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    List<WorkUnitState> previousWorkUnitStates = Collections.singletonList(new WorkUnitState());
    when(mockedState.getPreviousWorkUnitStates()).thenReturn(previousWorkUnitStates);
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(mockedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("onfull");
    when(mockedState.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY)).thenReturn("full");
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), previousWorkUnitStates);
  }
  
  /**
   * Test when work unit retry policy is on partial, and the job commit policy is "partial".
   */
  @Test
  public void testGetPreviousWorkUnitStatesOnPartialRetryPartialCommit() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    List<WorkUnitState> previousWorkUnitStates = Collections.singletonList(new WorkUnitState());
    when(mockedState.getPreviousWorkUnitStates()).thenReturn(previousWorkUnitStates);
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(mockedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("onpartial");
    when(mockedState.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY)).thenReturn("partial");
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), previousWorkUnitStates);
  }

  /**
   * Test the always-retry policy.
   */
  @Test
  public void testGetPreviousWorkUnitStatesAlwaysRetry() {
    TestSource testSource = new TestSource();
    SourceState mockedState = mock(SourceState.class);
    List<WorkUnitState> previousWorkUnitStates = Collections.singletonList(new WorkUnitState());
    when(mockedState.getPreviousWorkUnitStates()).thenReturn(previousWorkUnitStates);
    when(mockedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.FALSE);
    when(mockedState.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, Boolean.TRUE)).thenReturn(Boolean.TRUE);
    Assert.assertEquals(testSource.getPreviousWorkUnitStatesForRetry(mockedState), previousWorkUnitStates);
  }
  
  /**
   * Test the always-retry policy, and the overrite_configs_in_statestore enabled.
   * The previous workUnitState should be reset with the config in the current source.
   */
  @Test
  public void testGetPreviousWorkUnitStatesWithConfigOverWritten() {
    TestSource testSource = new TestSource();
    
    Properties oldProperties = new Properties();
    oldProperties.put("a", "1");
    oldProperties.put("b", "2");
    WorkUnitState previousWorkUnitState = new WorkUnitState();
    previousWorkUnitState.addAll(oldProperties);
    
    SourceState spiedState = spy(SourceState.class);
    
    List<WorkUnitState> previousWorkUnitStates = Collections.singletonList(previousWorkUnitState);
    when(spiedState.getPreviousWorkUnitStates()).thenReturn(previousWorkUnitStates);
    when(spiedState.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn(Boolean.TRUE);
    when(spiedState.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)).thenReturn("always");
    when(spiedState.getPropAsBoolean(ConfigurationKeys.OVERWRITE_CONFIGS_IN_STATESTORE,
        ConfigurationKeys.DEFAULT_OVERWRITE_CONFIGS_IN_STATESTORE)).thenReturn(Boolean.TRUE);
    Properties overwrittenProperties = new Properties();
    overwrittenProperties.put("a", "3");
    overwrittenProperties.put("b", "4");
    spiedState.addAll(overwrittenProperties);
    List<WorkUnitState> returnedPreviousWorkUnitStates = testSource.getPreviousWorkUnitStatesForRetry(spiedState);
    Assert.assertEquals(returnedPreviousWorkUnitStates, previousWorkUnitStates);
    Assert.assertEquals(previousWorkUnitState.getProp("a"), "3");
    Assert.assertEquals(previousWorkUnitState.getProp("b"), "4");
    Assert.assertEquals(returnedPreviousWorkUnitStates.get(0).getProp("a"), "3");
    Assert.assertEquals(returnedPreviousWorkUnitStates.get(0).getProp("b"), "4");
  }
 

  // Class for test AbstractSource
  public class TestSource extends AbstractSource{

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
