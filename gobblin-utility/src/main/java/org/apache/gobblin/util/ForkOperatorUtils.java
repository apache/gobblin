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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * Utility class for use with the {@link org.apache.gobblin.fork.ForkOperator} class.
 *
 * @author Yinan Li
 */
public class ForkOperatorUtils {

  /**
   * Get a new property key from an original one with branch index (if applicable).
   *
   * @param key      property key
   * @param numBranches number of branches (non-negative)
   * @param branchId    branch id (non-negative)
   * @return a new property key
   */
  public static String getPropertyNameForBranch(String key, int numBranches, int branchId) {
    Preconditions.checkArgument(numBranches >= 0, "The number of branches is expected to be non-negative");
    Preconditions.checkArgument(branchId >= 0, "The branchId is expected to be non-negative");
    return numBranches > 1 ? key + "." + branchId : key;
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
   * Get a new property key from an original one based on the branch id. The method assumes the branch id specified by
   * the {@link ConfigurationKeys#FORK_BRANCH_ID_KEY} parameter in the given WorkUnitState. The fork id key specifies
   * which fork this parameter belongs to. Note this method will only provide the aforementioned functionality for
   * {@link org.apache.gobblin.converter.Converter}s. To get the same functionality in {@link org.apache.gobblin.writer.DataWriter}s use
   * the {@link org.apache.gobblin.writer.DataWriterBuilder#forBranch(int)} to construct a writer with a specific branch id.
   *
   * @param workUnitState contains the fork id key
   * @param key           property key
   * @return a new property key
   */
  public static String getPropertyNameForBranch(WorkUnitState workUnitState, String key) {
    Preconditions.checkNotNull(workUnitState, "Cannot get a property from a null WorkUnit");
    Preconditions.checkNotNull(key, "Cannot get a the value for a null key");

    if (!workUnitState.contains(ConfigurationKeys.FORK_BRANCH_ID_KEY)) {
      return key;
    }
    return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY) >= 0
        ? key + "." + workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY) : key;
  }

  /**
   * Get a new path with the given branch name as a sub directory.
   *
   * @param numBranches number of branches (non-negative)
   * @param branchId    branch id (non-negative)
   * @return a new path
   */
  public static String getPathForBranch(State state, String path, int numBranches, int branchId) {
    Preconditions.checkNotNull(state);
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(numBranches >= 0, "The number of branches is expected to be non-negative");
    Preconditions.checkArgument(branchId >= 0, "The branch id is expected to be non-negative");

    return numBranches > 1
        ? path + Path.SEPARATOR + state.getProp(ConfigurationKeys.FORK_BRANCH_NAME_KEY + "." + branchId,
            ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + branchId)
        : path;
  }

  /**
   * Get the fork branch ID of a branch of a given task.
   *
   * @param taskId task ID
   * @param index  branch index
   * @return a fork branch ID
   */
  public static String getForkId(String taskId, int index) {
    return taskId + "." + index;
  }
}
