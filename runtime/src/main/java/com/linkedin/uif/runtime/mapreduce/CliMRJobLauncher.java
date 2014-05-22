package com.linkedin.uif.runtime.mapreduce;

import java.io.FileReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
        Properties jobProps = new Properties();
        // Last argument is the job configuration file
        jobProps.load(new FileReader(args[args.length - 1]));
        try {
            JobLauncher launcher = new MRJobLauncher(this.properties, getConf());
            launcher.launchJob(jobProps);
        } catch (JobException je) {
            return 1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        // Second to last argument is the framework configuration file
        properties.load(new FileReader(args[args.length - 2]));
        System.exit(ToolRunner.run(new Configuration(), new CliMRJobLauncher(properties), args));
    }
}
