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
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.GitConfigMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.RemoteInvocationException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
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
import org.apache.gobblin.service.FlowConfigClient;
import org.apache.gobblin.service.FlowConfigsResource;
import org.apache.gobblin.service.FlowConfigsResourceHandler;
import org.apache.gobblin.service.FlowConfigsV2Resource;
import org.apache.gobblin.service.FlowConfigsV2ResourceHandler;
import org.apache.gobblin.service.FlowExecutionResourceHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.GroupOwnershipService;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.db.ServiceDatabaseManager;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.topology.TopologySpecFactory;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.SpecStoreChangeMonitor;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
public class GobblinServiceManager implements ApplicationLauncher, StandardMetricsBridge {

  // Command line options
  // These two options are required to launch GobblinServiceManager.
  public static final String SERVICE_NAME_OPTION_NAME = "service_name";
  public static final String SERVICE_ID_OPTION_NAME = "service_id";

  public static final String SERVICE_EVENT_BUS_NAME = "GobblinServiceManagerEventBus";

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceManager.class);

  protected final ServiceBasedAppLauncher serviceLauncher;
  private volatile boolean stopInProgress = false;

  // An EventBus used for communications between services running in the ApplicationMaster
  @Inject
  @Named(SERVICE_EVENT_BUS_NAME)
  @Getter
  protected EventBus eventBus;

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
  protected FlowConfigsV2ResourceHandler v2ResourceHandler;

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

  protected boolean flowCatalogLocalCommit;

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
  protected Optional<HelixManager> helixManager;

  @Inject(optional = true)
  protected GitConfigMonitor gitConfigMonitor;

  @Inject(optional = true)
  @Getter
  protected DagManager dagManager;

  @Inject(optional = true)
  protected KafkaJobStatusMonitor jobStatusMonitor;

  @Inject
  protected MultiContextIssueRepository issueRepository;

  @Inject
  protected ServiceDatabaseManager databaseManager;

  @Inject(optional=true)
  @Getter
  protected Optional<UserQuotaManager> quotaManager;

  protected Optional<HelixLeaderState> helixLeaderGauges;

  @Inject(optional = true)
  protected D2Announcer d2Announcer;

  private final MetricContext metricContext;
  private final Metrics metrics;

  @Inject(optional = true)
  protected SpecStoreChangeMonitor specStoreChangeMonitor;

  @Inject(optional = true)
  protected DagActionStoreChangeMonitor dagActionStoreChangeMonitor;

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

    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState( configuration.getInnerConfig()), this.getClass());
    this.metrics = new Metrics(this.metricContext, configuration.getInnerConfig());
    this.serviceLauncher = new ServiceBasedAppLauncher(appLauncherProperties, configuration.getServiceName());

    this.fs = buildFileSystem(configuration.getInnerConfig());

    this.serviceWorkDir = ObjectUtils.firstNonNull(configuration.getServiceWorkDir(),
        getServiceWorkDirPath(this.fs, configuration.getServiceName(), configuration.getServiceId()));

    initializeHelixLeaderGauge();
  }

  public static GobblinServiceManager create(String serviceName, String serviceId, Config config,
      @Nullable Path serviceWorkDir) {
    return create(new GobblinServiceConfiguration(serviceName, serviceId, config, serviceWorkDir));
  }

  public static GobblinServiceManager create(GobblinServiceConfiguration serviceConfiguration) {
    GobblinServiceGuiceModule guiceModule = new GobblinServiceGuiceModule(serviceConfiguration);

    Injector injector = Guice.createInjector(Stage.PRODUCTION, guiceModule);
    return injector.getInstance(GobblinServiceManager.class);
  }

  public URI getRestLiServerListeningURI() {
    if (restliServer == null) {
      throw new IllegalStateException("Restli server does not exist because it was not configured or disabled");
    }
    return restliServer.getListeningURI();
  }

  private void initializeHelixLeaderGauge() {
    helixLeaderGauges = Optional.of(new HelixLeaderState());
    String helixLeaderStateGaugeName =
        MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.HELIX_LEADER_STATE);
    ContextAwareGauge<Integer> gauge = metricContext.newContextAwareGauge(helixLeaderStateGaugeName, () -> helixLeaderGauges.get().state.getValue());
    metricContext.register(helixLeaderStateGaugeName, gauge);
  }

  @VisibleForTesting
  public boolean isLeader() {
    // If helix manager is absent, then this standalone instance hence leader
    // .. else check if this master of cluster
    return !helixManager.isPresent() || helixManager.get().isLeader();
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



  /**
   * Handle leadership change.
   * @param changeContext notification context
   */
  private void handleLeadershipChange(NotificationContext changeContext) {
    if (this.helixManager.isPresent() && this.helixManager.get().isLeader()) {
      LOGGER.info("Leader notification for {} HM.isLeader {}", this.helixManager.get().getInstanceName(),
          this.helixManager.get().isLeader());

      if (configuration.isSchedulerEnabled()) {
        LOGGER.info("Gobblin Service is now running in master instance mode, enabling Scheduler.");
        this.scheduler.setActive(true);
      }

      if (helixLeaderGauges.isPresent()) {
        helixLeaderGauges.get().setState(LeaderState.MASTER);
      }

      if (configuration.isGitConfigMonitorEnabled()) {
        this.gitConfigMonitor.setActive(true);
      }

      if (configuration.isDagManagerEnabled()) {
        //Activate DagManager only if TopologyCatalog is initialized. If not; skip activation.
        if (this.topologyCatalog.getInitComplete().getCount() == 0) {
          this.dagManager.setActive(true);
          this.eventBus.register(this.dagManager);
        }
      }

      if (configuration.isOnlyAnnounceLeader()) {
        this.d2Announcer.markUpServer();
      }
    } else if (this.helixManager.isPresent()) {
      LOGGER.info("Leader lost notification for {} HM.isLeader {}", this.helixManager.get().getInstanceName(),
          this.helixManager.get().isLeader());

      if (configuration.isSchedulerEnabled()) {
        LOGGER.info("Gobblin Service is now running in slave instance mode, disabling Scheduler.");
        this.scheduler.setActive(false);
      }

      if (helixLeaderGauges.isPresent()) {
        helixLeaderGauges.get().setState(LeaderState.SLAVE);
      }

      if (configuration.isGitConfigMonitorEnabled()) {
        this.gitConfigMonitor.setActive(false);
      }

      if (configuration.isDagManagerEnabled()) {
        this.dagManager.setActive(false);
        this.eventBus.unregister(this.dagManager);
      }

      if (configuration.isOnlyAnnounceLeader()) {
        this.d2Announcer.markDownServer();
      }
    }
  }

  private void registerServicesInLauncher(){
    if (configuration.isTopologyCatalogEnabled()) {
      this.serviceLauncher.addService(topologyCatalog);
    }

    if (configuration.isFlowCatalogEnabled()) {
      this.serviceLauncher.addService(flowCatalog);

      if (configuration.isGitConfigMonitorEnabled()) {
        this.serviceLauncher.addService(gitConfigMonitor);
      }
    }

    if (configuration.isDagManagerEnabled()) {
      this.serviceLauncher.addService(dagManager);
    }

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

    if (this.configuration.isWarmStandbyEnabled()) {
      this.serviceLauncher.addService(specStoreChangeMonitor);
      this.serviceLauncher.addService(dagActionStoreChangeMonitor);
    }
  }

  private void configureServices(){
    if (configuration.isRestLIServerEnabled()) {
      this.restliServer = EmbeddedRestliServer.builder()
          .resources(Lists.newArrayList(FlowConfigsResource.class, FlowConfigsV2Resource.class))
          .injector(injector)
          .build();

      if (configuration.getInnerConfig().hasPath(ServiceConfigKeys.SERVICE_PORT)) {
        this.restliServer.setPort(configuration.getInnerConfig().getInt(ServiceConfigKeys.SERVICE_PORT));
      }
    }

    registerServicesInLauncher();

    // Register Scheduler to listen to changes in Flows
    // In warm standby mode, instead of scheduler we will add orchestrator as listener
    if(configuration.isWarmStandbyEnabled()) {
      this.flowCatalog.addListener(this.orchestrator);
    } else if (configuration.isSchedulerEnabled()) {
      this.flowCatalog.addListener(this.scheduler);
    }
  }

  private void ensureInjected() {
    if (v2ResourceHandler == null) {
      throw new IllegalStateException("GobblinServiceManager should be constructed through Guice dependency injection "
          + "or through a static factory method");
    }
  }

  @Override
  public void start() throws ApplicationException {
    LOGGER.info("[Init] Starting the Gobblin Service Manager");

    ensureInjected();

    configureServices();

    if (this.helixManager.isPresent()) {
      connectHelixManager();
    }

    this.eventBus.register(this);
    this.serviceLauncher.start();

    if (this.helixManager.isPresent()) {
      // Subscribe to leadership changes
      this.helixManager.get().addControllerListener((ControllerChangeListener) this::handleLeadershipChange);


      // Update for first time since there might be no notification
      if (helixManager.get().isLeader()) {
        if (configuration.isSchedulerEnabled()) {
          LOGGER.info("[Init] Gobblin Service is running in master instance mode, enabling Scheduler.");
          this.scheduler.setActive(true);
        }

        if (configuration.isGitConfigMonitorEnabled()) {
          this.gitConfigMonitor.setActive(true);
        }

        if (helixLeaderGauges.isPresent()) {
          helixLeaderGauges.get().setState(LeaderState.MASTER);
        }

      } else {
        if (configuration.isSchedulerEnabled()) {
          LOGGER.info("[Init] Gobblin Service is running in slave instance mode, not enabling Scheduler.");
        }
        if (helixLeaderGauges.isPresent()) {
          helixLeaderGauges.get().setState(LeaderState.SLAVE);
        }
      }
    } else {
      // No Helix manager, hence standalone service instance
      // .. designate scheduler to itself
      LOGGER.info("[Init] Gobblin Service is running in single instance mode, enabling Scheduler.");
      this.scheduler.setActive(true);

      if (configuration.isGitConfigMonitorEnabled()) {
        this.gitConfigMonitor.setActive(true);
      }
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

    //Activate the DagManager service, after the topologyCatalog has been initialized.
    if (!this.helixManager.isPresent() || this.helixManager.get().isLeader()){
      if (configuration.isDagManagerEnabled()) {
        this.dagManager.setActive(true);
        this.eventBus.register(this.dagManager);
      }
    }
  }

  @Override
  public void stop() throws ApplicationException {
    if (this.stopInProgress) {
      return;
    }

    LOGGER.info("Stopping the Gobblin Service Manager");
    this.stopInProgress = true;
    try {
      this.serviceLauncher.stop();
    } catch (ApplicationException ae) {
      LOGGER.error("Error while stopping Gobblin Service Manager", ae);
    } finally {
      disconnectHelixManager();
    }
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      if (this.helixManager.isPresent()) {
        this.helixManager.get().connect();
        this.helixManager.get()
            .getMessagingService()
            .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
                new ControllerUserDefinedMessageHandlerFactory(flowCatalogLocalCommit, scheduler, v2ResourceHandler,
                    configuration.getServiceName()));
      }
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  void disconnectHelixManager() {
    if (isHelixManagerConnected()) {
      if (this.helixManager.isPresent()) {
        this.helixManager.get().disconnect();
      }
    }
  }

  @VisibleForTesting
  boolean isHelixManagerConnected() {
    return this.helixManager.isPresent() && this.helixManager.get().isConnected();
  }

  @Override
  public void close() throws IOException {
    this.serviceLauncher.close();
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return ImmutableList.of(this.metrics);
  }

  private class Metrics extends StandardMetrics {
    public static final String SERVICE_LEADERSHIP_CHANGE = "serviceLeadershipChange";
    private ContextAwareHistogram serviceLeadershipChange;

    public Metrics(final MetricContext metricContext, Config config) {
      int timeWindowSizeInMinutes = ConfigUtils.getInt(config, ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES,
          ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);
      this.serviceLeadershipChange = metricContext.contextAwareHistogram(SERVICE_LEADERSHIP_CHANGE, timeWindowSizeInMinutes, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(this.serviceLeadershipChange);
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

      GobblinServiceConfiguration serviceConfiguration =
          new GobblinServiceConfiguration(cmd.getOptionValue(SERVICE_NAME_OPTION_NAME), getServiceId(cmd), config,
              null);

      GobblinServiceGuiceModule guiceModule = new GobblinServiceGuiceModule(serviceConfiguration);
      Injector injector = Guice.createInjector(guiceModule);

      try (GobblinServiceManager gobblinServiceManager = injector.getInstance(GobblinServiceManager.class)) {
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

    FlowConfigClient client =
        new FlowConfigClient(String.format("http://localhost:%s/", gobblinServiceManager.restliServer.getPort()));

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

    try {
      client.createFlowConfig(flowConfig);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }

  @Setter
  private static class HelixLeaderState {
    private LeaderState state = LeaderState.UNKNOWN;
  }

  private enum LeaderState {
    UNKNOWN(-1),
    SLAVE(0),
    MASTER(1);

    @Getter private int value;

    LeaderState(int value) {
      this.value = value;
    }
  }
}
