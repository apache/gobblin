package com.linkedin.uif.scheduler;

import com.linkedin.uif.configuration.ConfigurationKeys;

import java.io.IOException;

/**
 * An implementation of {@link TaskTracker} that reports
 * {@link TaskState}s to the {@link LocalJobManager}.
 *
 * <p>
 *     This is the implementation used only in single-node mode.
 * </p>
 *
 * @author ynli
 */
public class LocalTaskTracker implements TaskTracker {

    private final LocalJobManager jobManager;

    public LocalTaskTracker(LocalJobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void reportTaskState(TaskState state) throws IOException {
        String jobId = state.getProp(ConfigurationKeys.JOB_ID_KEY);
        this.jobManager.reportTaskState(jobId, state);
    }
}
