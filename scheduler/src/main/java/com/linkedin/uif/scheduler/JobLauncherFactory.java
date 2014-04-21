package com.linkedin.uif.scheduler;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.scheduler.local.LocalJobLauncher;
import com.linkedin.uif.scheduler.mapreduce.MRJobLauncher;

import java.util.Properties;

/**
 * A factory class for building {@link JobLauncher} instances.
 *
 * @author ynli
 */
public class JobLauncherFactory {

    /**
     * Suppported types of {@link JobLauncher}.
     */
    enum JobLauncherType {
        LOCAL,
        MAPREDUCE,
        YARN
    }

    /**
     * Create a new {@link JobLauncher}.
     *
     * @param properties Framework configuration properties
     * @return Newly created {@link JobLauncher}
     */
    public static JobLauncher newJobLauncher(Properties properties) throws Exception {
        JobLauncherType launcherType = JobLauncherType.valueOf(properties.getProperty(
                ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name()));
        switch (launcherType) {
            case LOCAL:
                return new LocalJobLauncher(properties);
            case MAPREDUCE:
                return new MRJobLauncher(properties);
            default:
                throw new RuntimeException(
                        "Unsupported job launcher type: " + launcherType.name());
        }
    }
}
