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

package org.apache.gobblin.azkaban;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import azkaban.jobExecutor.AbstractJob;
import javax.annotation.Nullable;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobLauncherFactory;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.listeners.CompositeJobListener;
import org.apache.gobblin.runtime.listeners.EmailNotificationJobListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.services.MetricsReportingService;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.TimeRangeChecker;
import org.apache.gobblin.util.hadoop.TokenUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;

import static org.apache.gobblin.runtime.AbstractJobLauncher.resolveGobblinJobTemplateIfNecessary;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * <p>
 *   By default, this class will use the {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher} to launch and run
 *   the Gobblin job unless a different job launcher type is explicitly specified in the job configuration
 *   using {@link ConfigurationKeys#JOB_LAUNCHER_TYPE_KEY}.
 * </p>
 *
 * <p>
 *   The launcher will use Hadoop token provided in environment variable
 *   {@link org.apache.hadoop.security.UserGroupInformation#HADOOP_TOKEN_FILE_LOCATION}.
 *   If it is missing, the launcher will get a token using {@link TokenUtils#getHadoopTokens}.
 * </p>
 *
 * @author Yinan Li
 */
public class AzkabanJobLauncher extends AbstractJob implements ApplicationLauncher, JobLauncher {

  private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

  public static final String GOBBLIN_LOG_LEVEL_KEY = "gobblin.log.levelOverride";
  public static final String LOG_LEVEL_OVERRIDE_MAP = "log.levelOverride.map";

  public static final String GOBBLIN_CUSTOM_JOB_LISTENERS = "gobblin.custom.job.listeners";

  private static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
  private static final String AZKABAN_LINK_JOBEXEC_URL = "azkaban.link.jobexec.url";
  private static final String AZKABAN_LINK_JOBEXEC_PROXY_URL = "azkaban.link.jobexec.proxyUrl";
  private static final String AZKABAN_FLOW_EXEC_ID = "azkaban.flow.execid";
  private static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

  private static final String AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS = "gobblin.azkaban.SLAInSeconds";
  private static final String DEFAULT_AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS = "-1"; // No SLA.

  public static final String GOBBLIN_AZKABAN_INITIALIZE_HADOOP_TOKENS = "gobblin.azkaban.initializeHadoopTokens";
  public static final String DEFAULT_GOBBLIN_AZKABAN_INITIALIZE_HADOOP_TOKENS = "true";

  private final Closer closer = Closer.create();
  private final JobLauncher jobLauncher;
  private final JobListener jobListener;

  private final Properties props;
  private final ApplicationLauncher applicationLauncher;
  private final long ownAzkabanSla;

  public AzkabanJobLauncher(String jobId, Properties props)
      throws Exception {
      super(jobId, LOG);

    HadoopUtils.addGobblinSite();

    // Configure root metric context
    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    RootMetricContext.get(tags);

    if (props.containsKey(GOBBLIN_LOG_LEVEL_KEY)) {
      Level logLevel = Level.toLevel(props.getProperty(GOBBLIN_LOG_LEVEL_KEY), Level.INFO);
      Logger.getLogger("org.apache.gobblin").setLevel(logLevel);
    }

    Log4jConfigurationHelper.setLogLevel(PropertiesUtils.getPropAsList(props, Log4jConfigurationHelper.LOG_LEVEL_OVERRIDE_MAP, ""));

    this.props = new Properties();
    this.props.putAll(props);

    // initialize job listeners after properties has been initialized
    this.jobListener = initJobListener();

    // load dynamic configuration and add them to the job properties
    Config propsAsConfig = ConfigUtils.propertiesToConfig(props);
    DynamicConfigGenerator dynamicConfigGenerator =
        DynamicConfigGeneratorFactory.createDynamicConfigGenerator(propsAsConfig);
    Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(propsAsConfig);

    // add the dynamic config to the job config
    for (Map.Entry<String, ConfigValue> entry : dynamicConfig.entrySet()) {
      this.props.put(entry.getKey(), entry.getValue().unwrapped().toString());
    }

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

    if (Boolean.parseBoolean(this.props.getProperty(GOBBLIN_AZKABAN_INITIALIZE_HADOOP_TOKENS,
        DEFAULT_GOBBLIN_AZKABAN_INITIALIZE_HADOOP_TOKENS))) {
      if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
        LOG.info("Job type " + props.getProperty(JOB_TYPE) + " provided Hadoop token in the environment variable " + HADOOP_TOKEN_FILE_LOCATION);
        this.props.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      } else {
        // see javadoc for more information
        LOG.info(
            "Job type " + props.getProperty(JOB_TYPE) + " did not provide Hadoop token in the environment variable " + HADOOP_TOKEN_FILE_LOCATION + ". Negotiating Hadoop tokens.");

        File tokenFile = Files.createTempFile("mr-azkaban", ".token").toFile();
        TokenUtils.getHadoopTokens(new State(props), Optional.of(tokenFile), new Credentials());

        System.setProperty(HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
        System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, tokenFile.getAbsolutePath());
        this.props.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, tokenFile.getAbsolutePath());
        this.props.setProperty("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
      }
    }

    Properties jobProps = this.props;
    resolveGobblinJobTemplateIfNecessary(jobProps);
    GobblinMetrics.addCustomTagsToProperties(jobProps, tags);

    // If the job launcher type is not specified in the job configuration,
    // override the default to use the MAPREDUCE launcher.
    if (!jobProps.containsKey(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY)) {
      jobProps.setProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY,
          JobLauncherFactory.JobLauncherType.MAPREDUCE.toString());
    }

    this.ownAzkabanSla = Long.parseLong(
        jobProps.getProperty(AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS, DEFAULT_AZKABAN_GOBBLIN_JOB_SLA_IN_SECONDS));

    List<? extends Tag<?>> metadataTags = Lists.newArrayList();
    //Is the job triggered using Gobblin-as-a-Service? If so, add additional tags needed for tracking
    //the job execution.
    if (jobProps.containsKey(ConfigurationKeys.FLOW_NAME_KEY)) {
      metadataTags = addAdditionalMetadataTags(jobProps);
    }

    // Create a JobLauncher instance depending on the configuration. The same properties object is
    // used for both system and job configuration properties because Azkaban puts configuration
    // properties in the .job file and in the .properties file into the same Properties object.
    this.jobLauncher = this.closer.register(JobLauncherFactory.newJobLauncher(jobProps, jobProps, null, metadataTags));

    // Since Java classes cannot extend multiple classes and Azkaban jobs must extend AbstractJob, we must use composition
    // verses extending ServiceBasedAppLauncher
    boolean isMetricReportingFailureFatal = PropertiesUtils
        .getPropAsBoolean(jobProps, ConfigurationKeys.GOBBLIN_JOB_METRIC_REPORTING_FAILURE_FATAL,
            Boolean.toString(ConfigurationKeys.DEFAULT_GOBBLIN_JOB_METRIC_REPORTING_FAILURE_FATAL));
    boolean isEventReportingFailureFatal = PropertiesUtils
        .getPropAsBoolean(jobProps, ConfigurationKeys.GOBBLIN_JOB_EVENT_REPORTING_FAILURE_FATAL,
            Boolean.toString(ConfigurationKeys.DEFAULT_GOBBLIN_JOB_EVENT_REPORTING_FAILURE_FATAL));

    jobProps.setProperty(MetricsReportingService.METRICS_REPORTING_FAILURE_FATAL_KEY, Boolean.toString(isMetricReportingFailureFatal));
    jobProps.setProperty(MetricsReportingService.EVENT_REPORTING_FAILURE_FATAL_KEY, Boolean.toString(isEventReportingFailureFatal));

    this.applicationLauncher =
        this.closer.register(new ServiceBasedAppLauncher(jobProps, "Azkaban-" + UUID.randomUUID()));
  }

  protected JobListener initJobListener() {
    CompositeJobListener compositeJobListener = new CompositeJobListener();
    List<String> listeners = new State(props).getPropAsList(GOBBLIN_CUSTOM_JOB_LISTENERS, EmailNotificationJobListener.class.getSimpleName());
    try {
      for (String listenerAlias: listeners) {
        ClassAliasResolver<JobListener> conditionClassAliasResolver = new ClassAliasResolver<>(JobListener.class);
        compositeJobListener.addJobListener(conditionClassAliasResolver.resolveClass(listenerAlias).newInstance());
      }
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }

    return compositeJobListener;
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


  /**
   * Add additional properties such as flow.group, flow.name, executionUrl. Useful for tracking
   * job executions on Azkaban triggered by Gobblin-as-a-Service (GaaS).
   * @param jobProps job properties
   * @return a list of tags uniquely identifying a job execution on Azkaban.
   */
  private static List<? extends Tag<?>> addAdditionalMetadataTags(Properties jobProps) {
    List<Tag<?>> metadataTags = Lists.newArrayList();
    String jobExecutionId = jobProps.getProperty(AZKABAN_FLOW_EXEC_ID, "");
    // Display the proxy URL in the metadata tag if it exists
    String jobExecutionUrl = jobProps.getProperty(AZKABAN_LINK_JOBEXEC_PROXY_URL, jobProps.getProperty(AZKABAN_LINK_JOBEXEC_URL, ""));

    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
        jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));

    if (jobProps.containsKey(ConfigurationKeys.JOB_CURRENT_ATTEMPTS)) {
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_ATTEMPTS, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_GENERATION, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD,
          "false"));
    }

    // use job execution id if flow execution id is not present
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, jobExecutionId)));

    //Use azkaban.flow.execid as the jobExecutionId
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, jobExecutionId));

    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.METADATA_MESSAGE, jobExecutionUrl));

    LOG.debug(String.format("AzkabanJobLauncher.addAdditionalMetadataTags: metadataTags %s", metadataTags));

    return metadataTags;
  }

}
