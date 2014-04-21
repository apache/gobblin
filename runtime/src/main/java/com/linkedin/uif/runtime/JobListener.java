package com.linkedin.uif.runtime;

/**
 * An interface for classes used for callback on job state changes.
 */
public interface JobListener {

    /**
     * Called when a job is completed.
     *
     * @param jobState Job state
     */
    public void jobCompleted(JobState jobState);
}
