package com.linkedin.uif.azkaban;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;

import azkaban.jobExecutor.AbstractJob;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.JobLauncher;
import com.linkedin.uif.runtime.JobLauncherFactory;
import com.linkedin.uif.runtime.mapreduce.MRJobLock;

/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class AzkabanJobLauncher extends AbstractJob {

    private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

    private final Properties properties;
    private final JobLauncher jobLauncher;

    public AzkabanJobLauncher(String jobId, Properties props) throws Exception {
        super(jobId, LOG);

        this.properties = props;
        final Configuration conf = new Configuration();

        String fsUri = conf.get("fs.default.name");
        if (!Strings.isNullOrEmpty(fsUri)) {
            if (!this.properties.containsKey(ConfigurationKeys.FS_URI_KEY)) {
                this.properties.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
            }
            if (!this.properties.containsKey(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)) {
                this.properties.setProperty(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, fsUri);
            }
            if (!this.properties.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
                this.properties.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
            }
        }

        // MR job lock should be used as the job will be run in a separate process
        this.properties.setProperty(
                ConfigurationKeys.LOCAL_USE_MR_JOB_LOCK_KEY, Boolean.toString(true));

        // Create a JobLauncher instance depending on the configuration
        this.jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);

        // Add a shutdown hook so the job lock file gets deleted even when the job is killed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                String jobLockFile = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY) +
                        MRJobLock.LOCK_FILE_EXTENSION;
                try {
                    FileSystem fs = FileSystem.get(conf);
                    Path jobLockPath = new Path(
                            properties.getProperty(ConfigurationKeys.MR_JOB_LOCK_DIR_KEY),
                            jobLockFile);
                    if (fs.exists(jobLockPath)) {
                        fs.delete(jobLockPath, false);
                    }
                } catch (IOException ioe) {
                    LOG.error("Failed to delete job lock file " + jobLockFile, ioe);
                }
            }
        });
    }

    @Override
    public void run() throws Exception {
        this.jobLauncher.launchJob(this.properties, null);
    }
}
