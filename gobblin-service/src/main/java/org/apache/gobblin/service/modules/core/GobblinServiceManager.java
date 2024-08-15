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

package org.apache.gobblin.service.modules.core;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.RemoteInvocationException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigV2Client;
import org.apache.gobblin.service.FlowConfigsV2Resource;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.GroupOwnershipService;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.db.ServiceDatabaseManager;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.restli.FlowConfigsResourceHandler;
import org.apache.gobblin.service.modules.restli.FlowExecutionResourceHandler;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.topology.TopologySpecFactory;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.GitConfigMonitor;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.SpecStoreChangeMonitor;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
public class GobblinServiceManager implements ApplicationLauncher, StandardMetricsBridge {

  // Command line options
  // These two options are required to launch GobblinServiceManager.
  public static final String SERVICE_NAME_OPTION_NAME = "service_name";
  public static final String SERVICE_ID_OPTION_NAME = "service_id";
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceManager.class);
  protected final ServiceBasedAppLauncher serviceLauncher;
  private volatile boolean stopInProgress = false;
  protected final FileSystem fs;
  protected final Path serviceWorkDir;

  @Getter
  protected final GobblinServiceConfiguration configuration;

  @Inject(optional = true)
  protected TopologyCatalog topologyCatalog;

  @Inject(optional = true)
  @Getter
  protected FlowCatalog flowCatalog;

  @Inject(optional = true)
  @Getter
  protected GobblinServiceJobScheduler scheduler;

  @Inject
  @Getter
  protected FlowConfigsResourceHandler resourceHandler;

  @Inject
  @Getter
  protected FlowExecutionResourceHandler flowExecutionResourceHandler;

  @Inject
  @Getter
  protected FlowStatusGenerator flowStatusGenerator;

  @Inject
  @Getter
  protected GroupOwnershipService groupOwnershipService;

  @Inject
  @Getter
  private Injector injector;
  @Getter @Setter private volatile static Injector staticInjector;

  @Inject(optional = true)
  @Getter
  protected Orchestrator orchestrator;

  @Inject(optional = true)
  protected EmbeddedRestliServer restliServer;

  @Inject(optional = true)
  protected TopologySpecFactory topologySpecFactory;

  @Inject
  protected SchedulerService schedulerService;

  @Inject(optional = true)
  protected GitConfigMonitor gitConfigMonitor;

  @Inject(optional = true)
  protected KafkaJobStatusMonitor jobStatusMonitor;

  @Inject
  protected MultiContextIssueRepository issueRepository;

  @Inject
  protected ServiceDatabaseManager databaseManager;

  @Inject
  @Getter
  protected UserQuotaManager quotaManager;

  @Inject(optional = true)
  protected D2Announcer d2Announcer;

  private final Metrics metrics;

  @Inject(optional = true)
  protected SpecStoreChangeMonitor specStoreChangeMonitor;

  @Inject(optional = true)
  protected DagActionStoreChangeMonitor dagActionStoreChangeMonitor;

  @Inject(optional = true)
  protected DagProcessingEngine dagProcessingEngine;

  @Inject
  protected GobblinServiceManager(GobblinServiceConfiguration configuration) throws Exception {
    this.configuration = Objects.requireNonNull(configuration);

    Properties appLauncherProperties = ConfigUtils.configToProperties(
        ConfigUtils.getConfigOrEmpty(configuration.getInnerConfig(), ServiceConfigKeys.GOBBLIN_SERVICE_APP_LAUNCHER_PREFIX)
            .withFallback(configuration.getInnerConfig()));

    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    if (!appLauncherProperties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      appLauncherProperties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }

    MetricContext metricContext =
        Instrumented.getMetricContext(ConfigUtils.configToState(configuration.getInnerConfig()), this.getClass());
    this.metrics = new Metrics(metricContext, configuration.getInnerConfig());
    this.serviceLauncher = new ServiceBasedAppLauncher(appLauncherProperties, configuration.getServiceName());

    this.fs = buildFileSystem(configuration.getInnerConfig());

    this.serviceWorkDir = ObjectUtils.firstNonNull(configuration.getServiceWorkDir(),
        getServiceWorkDirPath(this.fs, configuration.getServiceName(), configuration.getServiceId()));
  }

  public static GobblinServiceManager create(String serviceName, String serviceId, Config config,
      @Nullable Path serviceWorkDir) {
    return create(new GobblinServiceConfiguration(serviceName, serviceId, config, serviceWorkDir));
  }

  /**
   * Uses the provided serviceConfiguration to create a new Guice module and obtain a new class associated with it.
   */
  public static GobblinServiceManager create(GobblinServiceConfiguration serviceConfiguration) {
    if (GobblinServiceManager.staticInjector == null) {
        GobblinServiceManager.staticInjector = Guice.createInjector(Stage.DEVELOPMENT, new GobblinServiceGuiceModule(serviceConfiguration));
    }
    return getClass(GobblinServiceManager.staticInjector, GobblinServiceManager.class);
  }

  /**
   * This method assumes that {@link GobblinServiceManager} is created using guice, which sets the injector.
   * If it is created through other ways, caller should use {@link GobblinServiceManager#create(GobblinServiceConfiguration)}
   * or provide the {@link Injector} using {@link GobblinServiceManager#getClass(Injector, Class)} method instead.
   * @param classToGet
   * @return a new object if the class type is not marked with @Singleton, otherwise the same instance of the class
   * @param <T>
   */
  public static <T> T getClass(Class<T> classToGet) {
    return getClass(getStaticInjector(), classToGet);
  }

  public static <T> T getClass(Injector injector, Class<T> classToGet) {
    return injector.getInstance(classToGet);
  }

  public URI getRestLiServerListeningURI() {
    if (restliServer == null) {
      throw new IllegalStateException("Restli server does not exist because it was not configured or disabled");
    }
    return restliServer.getListeningURI();
  }

  private FileSystem buildFileSystem(Config config)
      throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), new Configuration())
        : FileSystem.get(new Configuration());
  }

  private Path getServiceWorkDirPath(FileSystem fs, String serviceName, String serviceId) {
    return new Path(fs.getHomeDirectory(), serviceName + Path.SEPARATOR + serviceId);
  }

  private void registerServicesInLauncher(){
    this.serviceLauncher.addService(topologyCatalog);

    if (configuration.isFlowCatalogEnabled()) {
      this.serviceLauncher.addService(flowCatalog);

      if (configuration.isGitConfigMonitorEnabled()) {
        this.serviceLauncher.addService(gitConfigMonitor);
      }
    }

    this.serviceLauncher.addService(dagProcessingEngine);

    this.serviceLauncher.addService(databaseManager);
    this.serviceLauncher.addService(issueRepository);

    if (configuration.isJobStatusMonitorEnabled()) {
      this.serviceLauncher.addService(jobStatusMonitor);
    }

    if (configuration.isSchedulerEnabled()) {
      this.serviceLauncher.addService(schedulerService);
      this.serviceLauncher.addService(scheduler);
    }

    if (configuration.isRestLIServerEnabled()) {
      this.serviceLauncher.addService(restliServer);
    }

    this.serviceLauncher.addService(specStoreChangeMonitor);
    this.serviceLauncher.addService(dagActionStoreChangeMonitor);
  }

  private void configureServices(){
    if (configuration.isRestLIServerEnabled()) {
      this.restliServer = EmbeddedRestliServer.builder()
          .resources(Lists.newArrayList(FlowConfigsV2Resource.class, FlowConfigsV2Resource.class))
          .injector(injector)
          .build();

      if (configuration.getInnerConfig().hasPath(ServiceConfigKeys.SERVICE_PORT)) {
        this.restliServer.setPort(configuration.getInnerConfig().getInt(ServiceConfigKeys.SERVICE_PORT));
      }
    }

    registerServicesInLauncher();

    // Register orchestrator to listen to changes in Flows
    this.flowCatalog.addListener(this.orchestrator);
  }

  @Override
  public void start() throws ApplicationException {
    LOGGER.info("[Init] Starting the Gobblin Service Manager");

    configureServices();
    this.serviceLauncher.start();

    LOGGER.info("[Init] Gobblin Service is running in master instance mode, enabling Scheduler.");
    this.scheduler.setActive(true);

    if (configuration.isGitConfigMonitorEnabled()) {
      this.gitConfigMonitor.setActive(true);
    }

    // Announce to d2 after services are initialized regardless of leadership if configuration is not enabled
    if (!this.configuration.isOnlyAnnounceLeader()) {
      this.d2Announcer.markUpServer();
    }

    // Populate TopologyCatalog with all Topologies generated by TopologySpecFactory
    // This has to be done after the topologyCatalog service is launched
    if (configuration.isTopologySpecFactoryEnabled()) {
      Collection<TopologySpec> topologySpecs = this.topologySpecFactory.getTopologies();
      for (TopologySpec topologySpec : topologySpecs) {
        this.topologyCatalog.put(topologySpec);
      }
    }

    // Register Orchestrator to listen to changes in topology
    // This has to be done after topologySpecFactory has updated spec store, so that listeners will have the latest updates.
    if (configuration.isSchedulerEnabled()) {
      this.topologyCatalog.addListener(this.orchestrator);
    }

    // Notify now topologyCatalog has the right information
    this.topologyCatalog.getInitComplete().countDown();

    //Activate the SpecCompiler, after the topologyCatalog has been initialized.
    this.orchestrator.getSpecCompiler().setActive(true);

    // Activate both monitors last as they're dependent on the SpecCompiler and Scheduler being active
    this.specStoreChangeMonitor.setActive();
    this.dagActionStoreChangeMonitor.setActive();
  }

  @Override
  public void stop() throws ApplicationException {
    if (this.stopInProgress) {
      return;
    }

    LOGGER.info("Stopping the Gobblin Service Manager");
    this.stopInProgress = true;
    try {
      // Stop announcing GaaS instances to d2 when services are stopped
      if (!configuration.isOnlyAnnounceLeader()) {
        this.d2Announcer.markDownServer();
      }
      this.serviceLauncher.stop();
    } catch (ApplicationException ae) {
      LOGGER.error("Error while stopping Gobblin Service Manager", ae);
    }
  }

  @Override
  public void close() throws IOException {
    this.serviceLauncher.close();
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return ImmutableList.of(this.metrics);
  }

  private static class Metrics extends StandardMetrics {
    public static final String SERVICE_LEADERSHIP_CHANGE = "serviceLeadershipChange";

    public Metrics(final MetricContext metricContext, Config config) {
      int timeWindowSizeInMinutes = ConfigUtils.getInt(config, ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES,
          ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);
      ContextAwareHistogram serviceLeadershipChange =
          metricContext.contextAwareHistogram(SERVICE_LEADERSHIP_CHANGE, timeWindowSizeInMinutes, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(serviceLeadershipChange);
    }
  }

  private static String getServiceId(CommandLine cmd) {
    return cmd.getOptionValue(SERVICE_ID_OPTION_NAME) == null ? "1" : cmd.getOptionValue(SERVICE_ID_OPTION_NAME);
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", SERVICE_NAME_OPTION_NAME, true, "Gobblin Service application's name");
    options.addOption("i", SERVICE_ID_OPTION_NAME, true, "Gobblin Service application's ID, "
        + "this needs to be globally unique");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinServiceManager.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(SERVICE_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      if (!cmd.hasOption(SERVICE_ID_OPTION_NAME)) {
        printUsage(options);
        LOGGER.warn("Please assign globally unique ID for a GobblinServiceManager instance, or it will use default ID");
      }

      boolean isTestMode = false;
      if (cmd.hasOption("test_mode")) {
        isTestMode = Boolean.parseBoolean(cmd.getOptionValue("test_mode", "false"));
      }

      Config config = ConfigFactory.load();

      try (GobblinServiceManager gobblinServiceManager =
          create(cmd.getOptionValue(SERVICE_NAME_OPTION_NAME), getServiceId(cmd), config, null)) {
        gobblinServiceManager.start();

        if (isTestMode) {
          testGobblinService(gobblinServiceManager);
        }
      }

    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }

  // TODO: Remove after adding test cases
  @SuppressWarnings("DLS_DEAD_LOCAL_STORE")
  private static void testGobblinService(GobblinServiceManager gobblinServiceManager) {

    try (FlowConfigV2Client client =
        new FlowConfigV2Client(String.format("http://localhost:%s/", gobblinServiceManager.restliServer.getPort()))) {

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    final String TEST_GROUP_NAME = "testGroup1";
    final String TEST_FLOW_NAME = "testFlow1";
    final String TEST_SCHEDULE = "0 1/0 * ? * *";
    final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    client.createFlowConfig(flowConfig);
    } catch (RemoteInvocationException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
