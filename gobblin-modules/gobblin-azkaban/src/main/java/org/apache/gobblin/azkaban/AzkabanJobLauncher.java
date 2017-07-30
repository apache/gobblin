/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.azkaban;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.RootMetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.app.ApplicationException;
import gobblin.runtime.app.ApplicationLauncher;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.runtime.listeners.EmailNotificationJobListener;
import gobblin.runtime.listeners.JobListener;
import gobblin.util.HadoopUtils;
import gobblin.util.TimeRangeChecker;
import gobblin.util.hadoop.TokenUtils;

import azkaban.jobExecutor.AbstractJob;
import javax.annotation.Nullable;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * <p>
 *   By default, this class will use the {@link gobblin.runtime.mapreduce.MRJobLauncher} to launch and run
 *   the Gobblin job unless a different job launcher type is explicitly specified in the job configuration
 *   using {@link ConfigurationKeys#JOB_LAUNCHER_TYPE_KEY}.
 * </p>
 *
 * <p>
 *   If the Azkaban job type is not contained in {@link #JOB_TYPES_WITH_AUTOMATIC_TOKEN}, the launcher assumes that
 *   the job does not get authentication tokens from Azkaban and it will negotiate them itself.
 *   See {@link TokenUtils#getHadoopTokens} for more information.
 * </p>
 *
 * @author Yinan Li
 */
public class AzkabanJobLauncher extends AbstractJob implements ApplicationLauncher, JobLauncher {

  private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

  public static final String GOBBLIN_LOG_LEVEL_KEY = "gobblin.log.levelOverride";

  private static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
  private static final String AZKABAN_LINK_JOBEXEC_URL = "azkaban.link.jobexec.url";
  private static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

  private static final String AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS = "gobblin.azkaban.SLAInSeconds";
  private static final String DEFAULT_AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS = "-1"; // No SLA.

  private static final String HADOOP_JAVA_JOB = "hadoopJava";
  private static final String JAVA_JOB = "java";
  private static final String GOBBLIN_JOB = "gobblin";
  private static final Set<String> JOB_TYPES_WITH_AUTOMATIC_TOKEN =
      Sets.newHashSet(HADOOP_JAVA_JOB, JAVA_JOB, GOBBLIN_JOB);

  private final Closer closer = Closer.create();
  private final JobLauncher jobLauncher;
  private final JobListener jobListener = new EmailNotificationJobListener();
  private final Properties props;
  private final ApplicationLauncher applicationLauncher;
  private final long ownAzkabanSla;

  public AzkabanJobLauncher(String jobId, Properties props)
      throws Exception {
    super(jobId, LOG);

    HadoopUtils.addGobblinSite();

    if (props.containsKey(GOBBLIN_LOG_LEVEL_KEY)) {
      Level logLevel = Level.toLevel(props.getProperty(GOBBLIN_LOG_LEVEL_KEY), Level.INFO);
      Logger.getLogger("gobblin").setLevel(logLevel);
    }

    this.props = new Properties();
    this.props.putAll(props);

    Configuration conf = new Configuration();

    String fsUri = conf.get(HADOOP_FS_DEFAULT_NAME);
    if (!Strings.isNullOrEmpty(fsUri)) {
      if (!this.props.containsKey(ConfigurationKeys.FS_URI_KEY)) {
        this.props.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
      }
      if (!this.props.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
        this.props.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
      }
    }

    // Set the job tracking URL to point to the Azkaban job execution link URL
    this.props
        .setProperty(ConfigurationKeys.JOB_TRACKING_URL_KEY, Strings.nullToEmpty(conf.get(AZKABAN_LINK_JOBEXEC_URL)));

    if (props.containsKey(JOB_TYPE) && JOB_TYPES_WITH_AUTOMATIC_TOKEN.contains(props.getProperty(JOB_TYPE))) {
      // Necessary for compatibility with Azkaban's hadoopJava job type
      // http://azkaban.github.io/azkaban/docs/2.5/#hadoopjava-type
      LOG.info(
          "Job type " + props.getProperty(JOB_TYPE) + " provides Hadoop tokens automatically. Using provided tokens.");
      if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
        this.props.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      }
    } else {
      // see javadoc for more information
      LOG.info(String.format("Job type %s does not provide Hadoop tokens. Negotiating Hadoop tokens.",
          props.getProperty(JOB_TYPE)));
      File tokenFile = TokenUtils.getHadoopTokens(new State(props));
      System.setProperty(HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
      System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, tokenFile.getAbsolutePath());
      this.props.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, tokenFile.getAbsolutePath());
      this.props.setProperty("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }

    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    RootMetricContext.get(tags);
    GobblinMetrics.addCustomTagsToProperties(this.props, tags);

    // If the job launcher type is not specified in the job configuration,
    // override the default to use the MAPREDUCE launcher.
    if (!this.props.containsKey(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY)) {
      this.props.setProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY,
          JobLauncherFactory.JobLauncherType.MAPREDUCE.toString());
    }

    this.ownAzkabanSla = Long.parseLong(
        this.props.getProperty(AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS, DEFAULT_AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS));

    // Create a JobLauncher instance depending on the configuration. The same properties object is
    // used for both system and job configuration properties because Azkaban puts configuration
    // properties in the .job file and in the .properties file into the same Properties object.
    this.jobLauncher = this.closer.register(JobLauncherFactory.newJobLauncher(this.props, this.props));

    // Since Java classes cannot extend multiple classes and Azkaban jobs must extend AbstractJob, we must use composition
    // verses extending ServiceBasedAppLauncher
    this.applicationLauncher =
        this.closer.register(new ServiceBasedAppLauncher(this.props, "Azkaban-" + UUID.randomUUID()));
  }

  @Override
  public void run()
      throws Exception {
    if (isCurrentTimeInRange()) {
      if (this.ownAzkabanSla > 0) {
        LOG.info("Found gobblin defined SLA: " + this.ownAzkabanSla);
        final ExecutorService service = Executors.newSingleThreadExecutor();
        boolean isCancelled = false;
        Future<Void> future = service.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            runRealJob();
            return null;
          }
        });

        try {
          future.get(this.ownAzkabanSla, TimeUnit.SECONDS);
        } catch (final TimeoutException e) {
          LOG.info("Cancelling job since SLA is reached: " + this.ownAzkabanSla);
          future.cancel(true);
          isCancelled = true;
          this.cancelJob(jobListener);
        } finally {
          service.shutdown();
          if (isCancelled) {
            this.cancel();
            // Need to fail the Azkaban job.
            throw new RuntimeException("Job failed because it reaches SLA limit: " + this.ownAzkabanSla);
          }
        }
      } else {
        runRealJob();
      }
    }
  }

  private void runRealJob()
      throws Exception {
    try {
      start();
      launchJob(jobListener);
    } finally {
      try {
        stop();
      } finally {
        close();
      }
    }
  }

  @Override
  public void cancel()
      throws Exception {
    try {
      stop();
    } finally {
      close();
    }
  }

  @Override
  public void start()
      throws ApplicationException {
    this.applicationLauncher.start();
  }

  @Override
  public void stop()
      throws ApplicationException {
    this.applicationLauncher.stop();
  }

  @Override
  public void launchJob(@Nullable JobListener jobListener)
      throws JobException {
    this.jobLauncher.launchJob(jobListener);
  }

  @Override
  public void cancelJob(@Nullable JobListener jobListener)
      throws JobException {
    this.jobLauncher.cancelJob(jobListener);
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }

  /**
   * Uses the properties {@link ConfigurationKeys#AZKABAN_EXECUTION_DAYS_LIST},
   * {@link ConfigurationKeys#AZKABAN_EXECUTION_TIME_RANGE}, and
   * {@link TimeRangeChecker#isTimeInRange(List, String, String, DateTime)} to determine if the current job should
   * continue its execution based on the extra scheduled parameters defined in the config.
   *
   * @return true if this job should be launched, false otherwise.
   */
  private boolean isCurrentTimeInRange() {
    Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();

    if (this.props.contains(ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST) && this.props
        .contains(ConfigurationKeys.AZKABAN_EXECUTION_TIME_RANGE)) {

      List<String> executionTimeRange =
          splitter.splitToList(this.props.getProperty(ConfigurationKeys.AZKABAN_EXECUTION_TIME_RANGE));
      List<String> executionDays =
          splitter.splitToList(this.props.getProperty(ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST));
      Preconditions.checkArgument(executionTimeRange.size() == 2,
          "The property " + ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST
              + " should be a comma separated list of two entries");

      return TimeRangeChecker.isTimeInRange(executionDays, executionTimeRange.get(0), executionTimeRange.get(1),
          new DateTime(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME)));
    }

    return true;
  }
}
