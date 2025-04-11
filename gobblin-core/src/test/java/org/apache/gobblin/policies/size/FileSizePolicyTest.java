package org.apache.gobblin.policies.size;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FileSizePolicyTest {

  @Test
  public void testPolicyPass() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 1000L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.PASSED));
  }

  @Test
  public void testPolicyFail() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 900L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.FAILED));
  }

  @Test
  public void testPolicyWithTolerance() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 999L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.FAILED));
  }
}