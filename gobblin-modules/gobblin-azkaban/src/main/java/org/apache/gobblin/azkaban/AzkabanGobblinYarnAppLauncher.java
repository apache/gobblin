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

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

import azkaban.jobExecutor.AbstractJob;
import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.util.AzkabanLauncherUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.GobblinYarnAppLauncher;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * A utility class for launching a Gobblin application on Yarn through Azkaban.
 *
 * <p>
 *   This class starts the driver of the Gobblin application on Yarn, which will be up running until the
 *   Azkaban job is killed/cancelled or the shutdown hook gets called and causes the driver to stop.
 * </p>
 *
 * <p>
 *   See {@link GobblinYarnAppLauncher} for details information on the launcher/driver of the Gobblin
 *   application on Yarn.
 * </p>
 *
 * @author Yinan Li
 */
public class AzkabanGobblinYarnAppLauncher extends AbstractJob {
  private static final Logger LOGGER = Logger.getLogger(AzkabanGobblinYarnAppLauncher.class);

  /** Config prefix for the {@link ServiceBasedAppLauncher} used for bootstrap-failure event reporting. */
  private static final String APP_LAUNCHER_PREFIX = "gobblinYarnAppLauncher";

  private final GobblinYarnAppLauncher gobblinYarnAppLauncher;
  private final Properties gobblinProps;
  private final ApplicationLauncher applicationLauncher;
  private final EventSubmitter eventSubmitter;

  @Getter
  protected final YarnConfiguration yarnConfiguration;

  public AzkabanGobblinYarnAppLauncher(String jobId, Properties gobblinProps) throws Exception {
    super(jobId, LOGGER);
    gobblinProps = AzkabanLauncherUtils.undoPlaceholderConversion(gobblinProps);
    addRuntimeProperties(gobblinProps);
    this.gobblinProps = gobblinProps;

    // Populate correct SSL keystore path from the Hadoop token file so Xinfra reporters work in
    // the GGW pod (XinfraDynamicConfigGenerator writes the cert to a temp file and sets the path)
    Config propsAsConfig = ConfigUtils.propertiesToConfig(gobblinProps);
    DynamicConfigGenerator dynamicConfigGenerator =
        DynamicConfigGeneratorFactory.createDynamicConfigGenerator(propsAsConfig);
    Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(propsAsConfig);
    for (Map.Entry<String, ConfigValue> entry : dynamicConfig.entrySet()) {
      gobblinProps.put(entry.getKey(), entry.getValue().unwrapped().toString());
    }

    this.applicationLauncher = buildApplicationLauncher(gobblinProps);
    this.eventSubmitter = buildEventSubmitter();

    Config gobblinConfig = ConfigUtils.propertiesToConfig(gobblinProps);
    setLogLevelForClasses(gobblinConfig);
    yarnConfiguration = initYarnConf(gobblinProps);
    gobblinConfig = gobblinConfig.withValue(GobblinYarnAppLauncher.GOBBLIN_YARN_APP_LAUNCHER_MODE,
        ConfigValueFactory.fromAnyRef(GobblinYarnAppLauncher.AZKABAN_APP_LAUNCHER_MODE_KEY));
    this.gobblinYarnAppLauncher = getYarnAppLauncher(gobblinConfig);
  }

  @VisibleForTesting
  AzkabanGobblinYarnAppLauncher(String jobId, Properties gobblinProps,
      GobblinYarnAppLauncher gobblinYarnAppLauncher, ApplicationLauncher applicationLauncher,
      EventSubmitter eventSubmitter) throws IOException {
    super(jobId, LOGGER);
    this.gobblinProps = gobblinProps;
    this.gobblinYarnAppLauncher = gobblinYarnAppLauncher;
    this.applicationLauncher = applicationLauncher;
    this.eventSubmitter = eventSubmitter;
    this.yarnConfiguration = new YarnConfiguration();
  }

  /**
   * Creates and starts a {@link ServiceBasedAppLauncher} scoped for bootstrap-failure event reporting.
   * Custom metric reporters that require YARN-container-specific SSL setup are temporarily removed
   * so the launcher starts cleanly in GGW pods, then restored for the actual Gobblin job launcher.
   */
  private ApplicationLauncher buildApplicationLauncher(Properties props) throws Exception {
    String customBuilders = props.containsKey(ConfigurationKeys.METRICS_CUSTOM_BUILDERS)
        ? (String) props.remove(ConfigurationKeys.METRICS_CUSTOM_BUILDERS) : null;
    try {
      Config fullConfig = ConfigUtils.propertiesToConfig(props);
      Config appConfig =
          ConfigUtils.getConfigOrEmpty(fullConfig, APP_LAUNCHER_PREFIX).withFallback(fullConfig);
      ApplicationLauncher launcher = new ServiceBasedAppLauncher(
          ConfigUtils.configToProperties(appConfig), "GobblinYarnAppLauncher-" + UUID.randomUUID());
      launcher.start();
      return launcher;
    } finally {
      if (customBuilders != null) {
        props.put(ConfigurationKeys.METRICS_CUSTOM_BUILDERS, customBuilders);
      }
    }
  }

  /**
   * Builds an {@link EventSubmitter} from the {@link MetricContext} wired up by
   * {@link #buildApplicationLauncher}, following the pattern used by {@code IvyJobLauncherTemporal}.
   */
  private EventSubmitter buildEventSubmitter() {
    MetricContext metricContext =
        Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    return new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
  }

  protected GobblinYarnAppLauncher getYarnAppLauncher(Config gobblinConfig)
      throws IOException {
    GobblinYarnAppLauncher gobblinYarnAppLauncher = new GobblinYarnAppLauncher(gobblinConfig, this.yarnConfiguration);
    gobblinYarnAppLauncher.initializeYarnClients(gobblinConfig);
    return gobblinYarnAppLauncher;
  }

  /**
   * Set Log Level for each class specified in the config. Class name and the corresponding log level can be specified
   * as "a:INFO,b:ERROR", where logs of class "a" are set to INFO and logs from class "b" are set to ERROR.
   * @param config
   */
  private void setLogLevelForClasses(Config config) {
    List<String> classLogLevels = ConfigUtils.getStringList(config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_AZKABAN_CLASS_LOG_LEVELS);

    for (String classLogLevel: classLogLevels) {
      String className = classLogLevel.split(":")[0];
      Level level = Level.toLevel(classLogLevel.split(":")[1], Level.INFO);
      Logger.getLogger(className).setLevel(level);
    }
  }

  /**
   * Extended class can override this method by providing their own YARN configuration.
   */
  protected YarnConfiguration initYarnConf(Properties gobblinProps) {
    return new YarnConfiguration();
  }

  /**
   * Extended class can override this method to add some runtime properties.
   */
  protected void addRuntimeProperties(Properties gobblinProps) {
  }

  @Override
  public void run() throws Exception {
    try {
      this.gobblinYarnAppLauncher.launch();
    } catch (Exception e) {
      LOGGER.error("Failed to launch the Gobblin Yarn application", e);
      submitJobFailedEvent(e);
      throw e;
    } finally {
      // Triggers MetricsReportingService shutdown which flushes events to Kafka before process exits.
      this.applicationLauncher.stop();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          AzkabanGobblinYarnAppLauncher.this.gobblinYarnAppLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timed out in shutting down the " + GobblinYarnAppLauncher.class.getSimpleName(), te);
        }
      }

    });
  }

  @Override
  public void cancel() throws Exception {
    this.gobblinYarnAppLauncher.stop();
  }

  /**
   * Emits a JOB_FAILED timing event and an {@link IssueEventBuilder} so GaaS transitions the job
   * to FAILED and populates {@code issues[]} with the bootstrap failure root cause.
   */
  private void submitJobFailedEvent(Exception e) {
    this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED).stop(getFlowMetadata());

    // Emit an IssueEventBuilder so the error details appear in the issues[] array in GaaS
    String summary = "YARN AM bootstrap failed: " + ExceptionUtils.getRootCauseMessage(e);
    Issue issue = Issue.builder()
        .time(ZonedDateTime.now(ZoneOffset.UTC))
        .severity(IssueSeverity.ERROR)
        .code("T" + DigestUtils.sha256Hex(summary).substring(0, 6).toUpperCase())
        .summary(summary)
        .details(ExceptionUtils.getStackTrace(e))
        .sourceClass(AzkabanGobblinYarnAppLauncher.class.getName())
        .properties(Collections.emptyMap())
        .build();
    IssueEventBuilder issueEventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);
    issueEventBuilder.setIssue(issue);
    issueEventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, ""));
    issueEventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY, ""));
    issueEventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ""));
    issueEventBuilder.addMetadata(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, ""));
    this.eventSubmitter.submit(issueEventBuilder);
  }

  /** Returns flow/job metadata tags from job properties for timing event metadata. */
  private Map<String, String> getFlowMetadata() {
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, ""));
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY, ""));
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ""));
    metadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, ""));
    metadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        this.gobblinProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, ""));
    return metadata;
  }
}
