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
     * Get all the previously failed/aborted work units that are
     * to be retried in the current run.
     *
     * <p>
     *     How work unit retry is handled is defined by {@link WorkUnitRetryPolicy}
     *     and {@link JobCommitPolicy}.
     * </p>
     *
     * @param state Source state
     * @return list of previously failed/aborted work units that are to be retried
     */
    protected List<WorkUnit> getPreviousWorkUnitsForRetry(SourceState state) {
        List<WorkUnit> previousWorkUnits = Lists.newArrayList();

        List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
        if(previousWorkUnitStates.isEmpty()) {
            return previousWorkUnits;
        }

        WorkUnitRetryPolicy workUnitRetryPolicy = WorkUnitRetryPolicy.forName(
                state.getProp(ConfigurationKeys.WORK_UNIT_RETRY_POLICY_KEY,
                        ConfigurationKeys.DEFAULT_WORK_UNIT_RETRY_POLICY));

        if (workUnitRetryPolicy == WorkUnitRetryPolicy.NEVER) {
            return previousWorkUnits;
        }

        // Get all the previously failed/aborted work units
        for (WorkUnitState workUnitState : previousWorkUnitStates) {
            if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.FAILED ||
                workUnitState.getWorkingState() == WorkUnitState.WorkingState.ABORTED) {
                previousWorkUnits.add(workUnitState.getWorkunit());
            }
        }

        if (workUnitRetryPolicy == WorkUnitRetryPolicy.ALWAYS) {
            return previousWorkUnits;
        }

        JobCommitPolicy jobCommitPolicy = JobCommitPolicy.forName(state.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
        if ((workUnitRetryPolicy == WorkUnitRetryPolicy.ON_PARTIAL_SUCCESS &&
                jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS) ||
            (workUnitRetryPolicy == WorkUnitRetryPolicy.ON_FULL_SUCCESS &&
                jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS)) {
            return previousWorkUnits;
        }

        return previousWorkUnits;
    }

}
