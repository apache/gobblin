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

package com.linkedin.uif.util;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * Utility class for use with the {@link com.linkedin.uif.fork.ForkOperator} class.
 *
 * @author ynli
 */
public class ForkOperatorUtils {

  /**
   * Get a branch name for the given branch.
   *
   * @param state       a {@link State} object carrying configuration properties
   * @param index       branch index
   * @param defaultName default branch name
   * @return a branch name
   */
  public static String getBranchName(State state, int index, String defaultName) {
    return state.getProp(ConfigurationKeys.FORK_BRANCH_NAME_KEY + "." + index, defaultName);
  }

  /**
   * Get a new property key from an original one with branch index (if applicable).
   *
   * @param key      property key
   * @param branches number of branches
   * @param index    branch index
   * @return a new property key
   */
  public static String getPropertyNameForBranch(String key, int branches, int index) {
    return branches > 1 ? key + "." + index : key;
  }

  /**
   * Get a new property key from an original one with branch index (if applicable).
   *
   * @param key    property key
   * @param branch branch index
   * @return a new property key
   */
  public static String getPropertyNameForBranch(String key, int branch) {
    // A branch index of -1 means there is no fork and branching
    return branch >= 0 ? key + "." + branch : key;
  }

  /**
   * Get a new path with the given branch name as a sub directory.
   *
   * @param branchName branch name
   * @param branches   number of branches
   * @return a new path
   */
  public static String getPathForBranch(String path, String branchName, int branches) {
    return branches > 1 ? path + "/" + branchName : path;
  }
}
