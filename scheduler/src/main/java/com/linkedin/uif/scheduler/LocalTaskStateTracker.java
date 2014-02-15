package com.linkedin.uif.scheduler;

import java.io.IOException;

/**
 * An implementation of {@link TaskStateTracker} that reports
 * {@link TaskState}s to the {@link LocalJobManager}.
 *
 * <p>
 *     This is the implementation used only in single-node mode.
 * </p>
 *
 * @author ynli
 */
public class LocalTaskStateTracker implements TaskStateTracker {

    private LocalJobManager jobManager;

    @Override
    public void reportTaskState(TaskState state) throws IOException {
        String jobId = state.getJobId();
        this.jobManager.reportTaskState(jobId, state);
    }

    /**
     * Set the {@link LocalJobManager} used by this {@link TaskStateTracker}.
     *
     * @param jobManager {@link LocalJobManager}
     */
    public void setJobManager(LocalJobManager jobManager) {
        this.jobManager = jobManager;
    }
}
