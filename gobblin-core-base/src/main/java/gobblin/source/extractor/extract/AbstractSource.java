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

package gobblin.source.extractor.extract;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.extractor.WorkUnitRetryPolicy;
import gobblin.source.workunit.ExtractFactory;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;


/**
 * A base implementation of {@link gobblin.source.Source} that provides default behavior.
 *
 * @author Yinan Li
 */
public abstract class AbstractSource<S, D> implements Source<S, D> {

  private final ExtractFactory extractFactory = new ExtractFactory("yyyyMMddHHmmss");

  /**
   * Get a list of {@link WorkUnitState}s of previous {@link WorkUnit}s subject for retries.
   *
   * <p>
   *     We use two keys for configuring work unit retries. The first one specifies
   *     whether work unit retries are enabled or not. This is for individual jobs
   *     or a group of jobs that following the same rule for work unit retries.
   *     The second one that is more advanced is for specifying a retry policy.
   *     This one is particularly useful for being a global policy for a group of
   *     jobs that have different job commit policies and want work unit retries only
   *     for a specific job commit policy. The first one probably is sufficient for
   *     most jobs that only need a way to enable/disable work unit retries. The
   *     second one gives users more flexibilities.
   * </p>
   *
   * @param state Source state
   * @return list of {@link WorkUnitState}s of previous {@link WorkUnit}s subject for retries
   */
  protected List<WorkUnitState> getPreviousWorkUnitStatesForRetry(SourceState state) {
    if (Iterables.isEmpty(state.getPreviousWorkUnitStates())) {
      return ImmutableList.of();
    }

    // Determine a work unit retry policy
    WorkUnitRetryPolicy workUnitRetryPolicy;
    if (state.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)) {
      // Use the given work unit retry policy if specified
      workUnitRetryPolicy = WorkUnitRetryPolicy.forName(state.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY));
    } else {
      // Otherwise set the retry policy based on if work unit retry is enabled
      boolean retryFailedWorkUnits = state.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, true);
      workUnitRetryPolicy = retryFailedWorkUnits ? WorkUnitRetryPolicy.ALWAYS : WorkUnitRetryPolicy.NEVER;
    }

    if (workUnitRetryPolicy == WorkUnitRetryPolicy.NEVER) {
      return ImmutableList.of();
    }

    List<WorkUnitState> previousWorkUnitStates = Lists.newArrayList();
    // Get previous work units that were not successfully committed (subject for retries)
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      if (workUnitState.getWorkingState() != WorkUnitState.WorkingState.COMMITTED) {
        if (state.getPropAsBoolean(ConfigurationKeys.OVERWRITE_CONFIGS_IN_STATESTORE,
            ConfigurationKeys.DEFAULT_OVERWRITE_CONFIGS_IN_STATESTORE)) {
          // We need to make a copy here since getPreviousWorkUnitStates returns ImmutableWorkUnitStates
          // for which addAll is not supported
          WorkUnitState workUnitStateCopy = new WorkUnitState(workUnitState.getWorkunit(), state);
          workUnitStateCopy.addAll(workUnitState);
          workUnitStateCopy.overrideWith(state);
          previousWorkUnitStates.add(workUnitStateCopy);
        } else {
          previousWorkUnitStates.add(workUnitState);
        }
      }
    }

    if (workUnitRetryPolicy == WorkUnitRetryPolicy.ALWAYS) {
      return previousWorkUnitStates;
    }

    JobCommitPolicy jobCommitPolicy = JobCommitPolicy
        .forName(state.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
    if ((workUnitRetryPolicy == WorkUnitRetryPolicy.ON_COMMIT_ON_PARTIAL_SUCCESS
        && jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS)
        || (workUnitRetryPolicy == WorkUnitRetryPolicy.ON_COMMIT_ON_FULL_SUCCESS
            && jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS)) {
      return previousWorkUnitStates;
    }
    // Return an empty list if job commit policy and work unit retry policy do not match
    return ImmutableList.of();
  }

  /**
   * Get a list of previous {@link WorkUnit}s subject for retries.
   *
   * <p>
   *     This method uses {@link AbstractSource#getPreviousWorkUnitStatesForRetry(SourceState)}.
   * </p>
   *
   * @param state Source state
   * @return list of previous {@link WorkUnit}s subject for retries
   */
  protected List<WorkUnit> getPreviousWorkUnitsForRetry(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (WorkUnitState workUnitState : getPreviousWorkUnitStatesForRetry(state)) {
      // Make a copy here as getWorkUnit() below returns an ImmutableWorkUnit
      workUnits.add(WorkUnit.copyOf(workUnitState.getWorkunit()));
    }

    return workUnits;
  }

  public Extract createExtract(TableType type, String namespace, String table) {
    return this.extractFactory.getUniqueExtract(type, namespace, table);
  }
}
