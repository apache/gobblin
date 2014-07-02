package com.linkedin.uif.source.extractor.extract;

import java.util.List;

import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.JobCommitPolicy;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.WorkUnitRetryPolicy;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * A base implementation of {@link com.linkedin.uif.source.Source}
 * that provides default behavior.
 *
 * @author ynli
 */
public abstract class AbstractSource<S, D> implements Source<S, D> {

    /**
     * Get all the previously uncommitted work units that are subject for retries.
     *
     * <p>
     *     We use two keys for configuring work unit retries. The first one specifies
     *     whether work unit retries are enabled or not. This is for individual jobs
     *     or a group of jobs that following the same rule for work unit retries.
     *     The second one that is more advanced is for specifying a retry policy.
     *     This one is particularly useful for being a global policy for a group of
     *     jobs that have different job commit policies and want work unit retries only
     *     for a specific job commit policy. The first one probably is sufficient for
     *     most jobs tnat only need a way to enable/disable work unit retries. The
     *     second one gives users more flexibilities.
     * </p>
     *
     * @param state Source state
     * @return list of previously failed/aborted work units that are to be retried
     */
    protected List<WorkUnit> getPreviousWorkUnitsForRetry(SourceState state) {
        List<WorkUnit> previousWorkUnits = Lists.newArrayList();

        List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
        if (previousWorkUnitStates.isEmpty()) {
            return previousWorkUnits;
        }

        // Determine a work unit retry policy
        WorkUnitRetryPolicy workUnitRetryPolicy;
        if (state.contains(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY)) {
            // Use the given work unit retry policy if specified
            workUnitRetryPolicy = WorkUnitRetryPolicy.forName(
                    state.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY));
        } else {
            // Otherwise set the retry policy based on if work unit retry is enabled
            boolean retryFailedWorkUnits = state.getPropAsBoolean(
                    ConfigurationKeys.WORK_UNIT_RETRY_ENABLED_KEY, true);
            workUnitRetryPolicy = retryFailedWorkUnits ?
                    WorkUnitRetryPolicy.ALWAYS : WorkUnitRetryPolicy.NEVER;
        }

        if (workUnitRetryPolicy == WorkUnitRetryPolicy.NEVER) {
            return previousWorkUnits;
        }

        // Get previous work units that were not successfully committed (subject for retries)
        for (WorkUnitState workUnitState : previousWorkUnitStates) {
            if (workUnitState.getWorkingState() != WorkUnitState.WorkingState.COMMITTED) {
                previousWorkUnits.add(workUnitState.getWorkunit());
            }
        }

        if (workUnitRetryPolicy == WorkUnitRetryPolicy.ALWAYS) {
            return previousWorkUnits;
        }

        JobCommitPolicy jobCommitPolicy = JobCommitPolicy.forName(state.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
        if ((workUnitRetryPolicy == WorkUnitRetryPolicy.ON_COMMIT_ON_PARTIAL_SUCCESS &&
                jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS) ||
            (workUnitRetryPolicy == WorkUnitRetryPolicy.ON_COMMIT_ON_FULL_SUCCESS &&
                jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS)) {
            return previousWorkUnits;
        }

        return previousWorkUnits;
    }

}
