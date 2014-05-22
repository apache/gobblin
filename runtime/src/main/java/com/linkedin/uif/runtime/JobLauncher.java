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
     * @throws JobException
     */
    public void launchJob(Properties jobProps) throws JobException;
}
