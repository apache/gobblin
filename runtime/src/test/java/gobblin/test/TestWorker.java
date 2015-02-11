/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.test;

import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.runtime.JobException;
import gobblin.runtime.JobListener;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.WorkUnitManager;
import gobblin.runtime.local.LocalJobManager;
import gobblin.runtime.local.LocalTaskStateTracker;


/**
 * A command-line utility for running tests of individual jobs.
 *
 * @author ynli
 */
@Deprecated
public class TestWorker {

  // We use this to manage all services running within the worker
  private final ServiceManager serviceManager;

  private final LocalJobManager jobManager;

  public TestWorker(Properties properties)
      throws Exception {
    // The worker runs the following services
    TaskExecutor taskExecutor = new TaskExecutor(properties);
    TaskStateTracker taskStateTracker = new LocalTaskStateTracker(properties, taskExecutor);
    WorkUnitManager workUnitManager = new WorkUnitManager(taskExecutor, taskStateTracker);
    this.jobManager = new LocalJobManager(workUnitManager, properties);
    ((LocalTaskStateTracker) taskStateTracker).setJobManager(this.jobManager);

    this.serviceManager = new ServiceManager(Lists.newArrayList(
        // The order matters due to dependencies between services
        taskExecutor, taskStateTracker, workUnitManager, jobManager));
  }

  /**
   * Start the worker.
   */
  public void start() {
    this.serviceManager.startAsync();
  }

  /**
   * Stop the worker.
   *
   * @throws TimeoutException
   */
  public void stop()
      throws TimeoutException {
    this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
  }

  /**
   * Run (or schedule, depending on the given mode) a job.
   *
   * @param jobProps Job configuration properties
   * @param mode Run mode
   * @param jobListener Job listener called when the job is done
   * @throws JobException
   */
  public void runJob(Properties jobProps, Mode mode, JobListener jobListener)
      throws JobException {

    switch (mode) {
      case RUN:
        this.jobManager.runJob(jobProps, jobListener);
        break;
      case SCHEDULE:
        this.jobManager.scheduleJob(jobProps, jobListener);
        break;
      default:
        throw new RuntimeException("Unsupported mode " + mode.name());
    }
  }

  /**
   * Print usage information.
   *
   * @param options Command-line options
   */
  public static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("TestWorker", options);
  }

  @SuppressWarnings("all")
  public static void main(String[] args)
      throws Exception {
    // Build command-line options
    Option configOption = OptionBuilder.withArgName("framework config file")
        .withDescription("Configuration properties file for the framework").hasArgs().withLongOpt("config").create('c');
    Option jobConfigsOption =
        OptionBuilder.withArgName("job config files").withDescription("Comma-separated list of job configuration files")
            .hasArgs().withLongOpt("jobconfigs").create('j');
    Option modeOption = OptionBuilder.withArgName("run mode").withDescription(
        "Test mode (schedule|run); 'schedule' means scheduling the jobs, "
            + "whereas 'run' means running the jobs immediately").hasArg().withLongOpt("mode").create('m');
    Option helpOption =
        OptionBuilder.withArgName("help").withDescription("Display usage information").withLongOpt("help").create('h');

    Options options = new Options();
    options.addOption(configOption);
    options.addOption(jobConfigsOption);
    options.addOption(modeOption);
    options.addOption(helpOption);

    // Parse command-line options
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption('h')) {
      printUsage(options);
      System.exit(0);
    }

    // Start the test worker with the given configuration properties
    Configuration config = new PropertiesConfiguration(cmd.getOptionValue('c'));
    Properties properties = ConfigurationConverter.getProperties(config);
    TestWorker testWorker = new TestWorker(properties);
    testWorker.start();

    // Job running mode
    Mode mode = Mode.valueOf(cmd.getOptionValue('m').toUpperCase());

    // Get the list of job configuration files
    List<String> jobConfigFiles =
        Lists.newArrayList(Splitter.on(',').omitEmptyStrings().trimResults().split(cmd.getOptionValue('j')));

    CountDownLatch latch = new CountDownLatch(jobConfigFiles.size());
    for (String jobConfigFile : jobConfigFiles) {
      // For each job, load the job configuration, then run or schedule the job.
      Properties jobProps = new Properties();
      jobProps.load(new FileReader(jobConfigFile));
      jobProps.putAll(properties);
      testWorker.runJob(jobProps, mode, new TestJobListener(latch));
    }
    // Wait for all jobs to finish
    latch.await();

    testWorker.stop();
  }

  /**
   * Job running modes
   */
  private static enum Mode {
    SCHEDULE,
    RUN
  }

  /**
   * An implementation of {@link JobListener} that counts down a latch when the job finishes.
   */
  private static class TestJobListener implements JobListener {

    private final CountDownLatch latch;

    public TestJobListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void jobCompleted(JobState jobState) {
      // Count down to indicate this job is done
      latch.countDown();
    }
  }
}
