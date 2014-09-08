package com.linkedin.uif.runtime;

import java.util.Properties;

/**
 * An interface for classes that launch a Gobblin job.
 *
 * @author ynli
 */
public interface JobLauncher {

    /**
     * Launch a Gobblin job.
     *
     * @param jobProps Job configuration properties
     * @param jobListener {@link JobListener} used for callback,
     *                    can be <em>null</em> if no callback is needed.
     * @throws JobException
     */
    public void launchJob(Properties jobProps, JobListener jobListener) throws JobException;

    /**
     * Cancel a Gobblin job.
     *
     * @param jobProps Job configuration properties
     * @throws JobException
     */
    public void cancelJob(Properties jobProps) throws JobException;
}
