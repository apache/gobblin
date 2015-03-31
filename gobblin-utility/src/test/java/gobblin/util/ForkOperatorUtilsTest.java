/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Unit tests for {@link ForkOperatorUtils}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.util"})
public class ForkOperatorUtilsTest {

  private static final String FORK_BRANCH_NAME_0 = "fork_foo_0";
  private static final String FORK_BRANCH_NAME_1 = "fork_foo_1";
  private static final String PROPERTY_FOO = "foo";
  private static final String PATH_FOO = "foo";

  @Test
  public void testGetBranchName() {
    State state = new State();
    state.setProp(ConfigurationKeys.FORK_BRANCH_NAME_KEY + ".0", FORK_BRANCH_NAME_0);
    state.setProp(ConfigurationKeys.FORK_BRANCH_NAME_KEY + ".1", FORK_BRANCH_NAME_1);
    Assert.assertEquals(ForkOperatorUtils.getBranchName(state, 0, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + 0),
        FORK_BRANCH_NAME_0);
    Assert.assertEquals(ForkOperatorUtils.getBranchName(state, 1, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + 1),
        FORK_BRANCH_NAME_1);
    Assert.assertEquals(ForkOperatorUtils.getBranchName(state, 2, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + 2),
        ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + 2);
  }

  @Test
  public void testGetPropertyNameForBranch() {
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, -1), PROPERTY_FOO);
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 0), PROPERTY_FOO + ".0");
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 1), PROPERTY_FOO + ".1");

    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 0, 0), PROPERTY_FOO);
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 1, 0), PROPERTY_FOO);
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 2, 0), PROPERTY_FOO + ".0");
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(PROPERTY_FOO, 2, 1), PROPERTY_FOO + ".1");
  }

  @Test
  public void testGetPathForBranch() {
    Assert.assertEquals(ForkOperatorUtils.getPathForBranch(PATH_FOO, FORK_BRANCH_NAME_0, 0), PATH_FOO);
    Assert.assertEquals(ForkOperatorUtils.getPathForBranch(PATH_FOO, FORK_BRANCH_NAME_0, 1), PATH_FOO);
    Assert.assertEquals(ForkOperatorUtils.getPathForBranch(PATH_FOO, FORK_BRANCH_NAME_0, 2),
        PATH_FOO + "/" + FORK_BRANCH_NAME_0);
    Assert.assertEquals(ForkOperatorUtils.getPathForBranch(PATH_FOO, FORK_BRANCH_NAME_1, 2),
        PATH_FOO + "/" + FORK_BRANCH_NAME_1);
  }

  /**
   * Test for {@link ForkOperatorUtils#getPropertyNameForBranch(WorkUnitState, String)}.
   */
  @Test
  public void testGetPropertyNameForBranchWithWorkUnitState() {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(PROPERTY_FOO, PATH_FOO);

    // Test that if the fork id key is not specified that the original property is preserved
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(workUnitState, PROPERTY_FOO), PROPERTY_FOO);

    // Test that if the fork id key is set to -1 that the original property is preserved
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, -1);
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(workUnitState, PROPERTY_FOO), PROPERTY_FOO);

    // Test that if the fork id key is set to 0 that the new property is properly created
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, 0);
    Assert.assertEquals(ForkOperatorUtils.getPropertyNameForBranch(workUnitState, PROPERTY_FOO), PROPERTY_FOO
        + ".0");
  }
}
