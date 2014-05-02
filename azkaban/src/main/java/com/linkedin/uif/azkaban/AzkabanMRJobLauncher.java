package com.linkedin.uif.azkaban;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.JobLauncher;
import com.linkedin.uif.runtime.mapreduce.MRJobLauncher;

/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * @author ynli
 */
public class AzkabanMRJobLauncher extends AbstractJob {

    private static final Logger LOG = Logger.getLogger(AzkabanMRJobLauncher.class);

    private final Properties properties;
    private final JobLauncher jobLauncher;

    public AzkabanMRJobLauncher(String jobId, Properties properties) throws Exception {
        super(jobId, LOG);

        this.properties = properties;
        Configuration conf = new Configuration();
        String fsUri = conf.get("fs.default.name");
        if (!this.properties.containsKey(ConfigurationKeys.FS_URI_KEY)) {
            this.properties.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
        }
        if (!this.properties.containsKey(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)) {
            this.properties.setProperty(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, fsUri);
        }
        if (!this.properties.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
            this.properties.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
        }

        this.jobLauncher = new MRJobLauncher(this.properties);
    }

    @Override
    public void run() throws Exception {
        this.jobLauncher.launchJob(this.properties);
    }
}
