package org.apache.gobblin.policies.size;

import org.apache.gobblin.configuration.State;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FileSizePolicyTest {

  @Test
  public void testPolicyPass() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 1000L);
    
    FileSizePolicy policy = new FileSizePolicy(state);
    Assert.assertTrue(policy.executePolicy().getResult());
  }

  @Test
  public void testPolicyFail() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 900L);
    
    FileSizePolicy policy = new FileSizePolicy(state);
    Assert.assertFalse(policy.executePolicy().getResult());
  }

  @Test
  public void testPolicyWithTolerance() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 999L);
    state.setProp(FileSizePolicy.SIZE_TOLERANCE_KEY, 0.01); // 1% tolerance
    
    FileSizePolicy policy = new FileSizePolicy(state);
    Assert.assertTrue(policy.executePolicy().getResult());
  }
} 