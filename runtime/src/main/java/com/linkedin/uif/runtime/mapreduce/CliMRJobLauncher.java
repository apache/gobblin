package com.linkedin.uif.runtime.mapreduce;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.JobException;
import com.linkedin.uif.runtime.JobLauncher;

/**
 * A utility class for launching a Gobblin Hadoop MR job through the command line.
 *
 * @author ynli
 */
public class CliMRJobLauncher extends Configured implements Tool {

    private final Properties properties;

    public CliMRJobLauncher(Properties properties) throws Exception {
        this.properties = properties;
    }

    @Override
    public int run(String[] args) throws Exception {
        final Properties jobProps = new Properties();
        // First load framework configuration properties
        jobProps.putAll(this.properties);
        // Then load job configuration properties. Last argument is the job configuration file.
        jobProps.load(new FileReader(args[args.length - 1]));

        // Add a shutdown hook so the job lock file gets deleted even when the job is killed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                String jobLockFile = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY) +
                        MRJobLock.LOCK_FILE_EXTENSION;
                try {
                    FileSystem fs = FileSystem.get(getConf());
                    Path jobLockPath = new Path(
                            jobProps.getProperty(ConfigurationKeys.MR_JOB_LOCK_DIR_KEY),
                            jobLockFile);
                    if (fs.exists(jobLockPath)) {
                        fs.delete(jobLockPath, false);
                    }
                } catch (IOException ioe) {
                    System.err.println("Failed to delete job lock file " + jobLockFile);
                }
            }
        });

        try {
            JobLauncher launcher = new MRJobLauncher(this.properties, getConf());
            launcher.launchJob(jobProps, null);
        } catch (JobException je) {
            return 1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Second to last argument is the framework configuration file
        String configFile = args[args.length - 2];
        // Load framework configuration properties
        Properties properties =  ConfigurationConverter.getProperties(
                new PropertiesConfiguration(configFile));
        System.exit(ToolRunner.run(
                new Configuration(), new CliMRJobLauncher(properties), args));
    }
}
