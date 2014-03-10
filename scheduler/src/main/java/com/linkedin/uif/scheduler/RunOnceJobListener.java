package com.linkedin.uif.scheduler;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Files;

import com.linkedin.uif.configuration.ConfigurationKeys;

/**
 * An implementation of {@link JobListener} for run-once jobs.
 *
 * @author ynli
 */
public class RunOnceJobListener implements JobListener {

    private static final Log LOG = LogFactory.getLog(RunOnceJobListener.class);

    @Override
    public void jobCompleted(JobState jobState) {
        String jobConfigFile = jobState.getProp(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
        // Rename the config file so we won't run this job when the worker is bounced
        try {
            Files.move(new File(jobConfigFile), new File(jobConfigFile + ".done"));
        } catch (IOException ioe) {
            LOG.error("Failed to rename job configuration file for job " +
                    jobState.getJobName(), ioe);
        }
    }
}
