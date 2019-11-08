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
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;

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
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigClient;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigV2ResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigsResource;
import org.apache.gobblin.service.FlowConfigsResourceHandler;
import org.apache.gobblin.service.FlowConfigsV2Resource;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.NoopRequesterService;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.restli.GobblinServiceFlowConfigResourceHandler;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.topology.TopologySpecFactory;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitorFactory;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Alpha
public class GobblinServiceManager implements ApplicationLauncher, StandardMetricsBridge {

  // Command line options
  // These two options are required to launch GobblinServiceManager.
  public static final String SERVICE_NAME_OPTION_NAME = "service_name";
  public static final String SERVICE_ID_OPTION_NAME = "service_id";

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceManager.class);
  private static final String JOB_STATUS_RETRIEVER_CLASS_KEY = "jobStatusRetriever.class";

  protected final ServiceBasedAppLauncher serviceLauncher;
  private volatile boolean stopInProgress = false;

  // An EventBus used for communications between services running in the ApplicationMaster
  protected final EventBus eventBus = new EventBus(GobblinServiceManager.class.getSimpleName());

  protected final FileSystem fs;
  protected final Path serviceWorkDir;
  protected final String serviceName;
  protected final String serviceId;

  protected final boolean isTopologyCatalogEnabled;
  protected final boolean isFlowCatalogEnabled;
  protected final boolean isSchedulerEnabled;
  protected final boolean isRestLIServerEnabled;
  protected final boolean isTopologySpecFactoryEnabled;
  protected final boolean isGitConfigMonitorEnabled;
  protected final boolean isDagManagerEnabled;
  protected final boolean isJobStatusMonitorEnabled;

  protected TopologyCatalog topologyCatalog;
  @Getter
  protected FlowCatalog flowCatalog;
  @Getter
  protected GobblinServiceJobScheduler scheduler;
  @Getter
  protected GobblinServiceFlowConfigResourceHandler resourceHandler;
  @Getter
  protected GobblinServiceFlowConfigResourceHandler v2ResourceHandler;

  protected boolean flowCatalogLocalCommit;
  @Getter
  protected Orchestrator orchestrator;
  protected EmbeddedRestliServer restliServer;
  protected TopologySpecFactory topologySpecFactory;

  protected Optional<HelixManager> helixManager;

  protected ClassAliasResolver<TopologySpecFactory> aliasResolver;

  protected GitConfigMonitor gitConfigMonitor;

  protected DagManager dagManager;

  protected KafkaJobStatusMonitor jobStatusMonitor;

  @Getter
  protected Config config;
  private final MetricContext metricContext;
  private final Metrics metrics;

  public GobblinServiceManager(String serviceName, String serviceId, Config config,
      Optional<Path> serviceWorkDirOptional) throws Exception {

    Properties appLauncherProperties = ConfigUtils.configToProperties(ConfigUtils.getConfigOrEmpty(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_APP_LAUNCHER_PREFIX).withFallback(config));
    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    if (!appLauncherProperties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      appLauncherProperties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }
    this.config = config;
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.getClass());
    this.metrics = new Metrics(this.metricContext, config);
    this.serviceName = serviceName;
    this.serviceId = serviceId;
    this.serviceLauncher = new ServiceBasedAppLauncher(appLauncherProperties, serviceName);

    this.fs = buildFileSystem(config);
    this.serviceWorkDir = serviceWorkDirOptional.isPresent() ? serviceWorkDirOptional.get()
        : getServiceWorkDirPath(this.fs, serviceName, serviceId);

    // Initialize TopologyCatalog
    this.isTopologyCatalogEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_TOPOLOGY_CATALOG_ENABLED_KEY, true);
    if (isTopologyCatalogEnabled) {
      this.topologyCatalog = new TopologyCatalog(config, Optional.of(LOGGER));
      this.serviceLauncher.addService(topologyCatalog);
    }

    // Initialize FlowCatalog
    this.isFlowCatalogEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_FLOW_CATALOG_ENABLED_KEY, true);
    if (isFlowCatalogEnabled) {
      this.flowCatalog = new FlowCatalog(config, Optional.of(LOGGER));
      this.flowCatalogLocalCommit = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT,
          ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT);
      this.serviceLauncher.addService(flowCatalog);

      this.isGitConfigMonitorEnabled = ConfigUtils.getBoolean(config,
          ServiceConfigKeys.GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY, false);

      if (this.isGitConfigMonitorEnabled) {
        this.gitConfigMonitor = new GitConfigMonitor(config, this.flowCatalog);
        this.serviceLauncher.addService(this.gitConfigMonitor);
      }
    } else {
      this.isGitConfigMonitorEnabled = false;
    }

    // Initialize Helix
    Optional<String> zkConnectionString = Optional.fromNullable(ConfigUtils.getString(config,
        ServiceConfigKeys.ZK_CONNECTION_STRING_KEY, null));
    if (zkConnectionString.isPresent()) {
      LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);
      // This will create and register a Helix controller in ZooKeeper
      this.helixManager = Optional.fromNullable(buildHelixManager(config, zkConnectionString.get()));
    } else {
      LOGGER.info("No ZooKeeper connection string. Running in single instance mode.");
      this.helixManager = Optional.absent();
    }

    this.isDagManagerEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY, false);
    // Initialize DagManager
    if (this.isDagManagerEnabled) {
      this.dagManager = new DagManager(config);
      this.serviceLauncher.addService(this.dagManager);
    }

    this.isJobStatusMonitorEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY, true) ;
    // Initialize JobStatusMonitor
    if (this.isJobStatusMonitorEnabled) {
      this.jobStatusMonitor = new KafkaJobStatusMonitorFactory().createJobStatusMonitor(config);
      this.serviceLauncher.addService(this.jobStatusMonitor);
    }

    // Initialize ServiceScheduler
    this.isSchedulerEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_SCHEDULER_ENABLED_KEY, true);
    if (isSchedulerEnabled) {
      this.orchestrator = new Orchestrator(config, Optional.of(this.topologyCatalog), Optional.fromNullable(this.dagManager), Optional.of(LOGGER));
      SchedulerService schedulerService = new SchedulerService(ConfigUtils.configToProperties(config));

      this.scheduler = new GobblinServiceJobScheduler(this.serviceName, config, this.helixManager,
          Optional.of(this.flowCatalog), Optional.of(this.topologyCatalog), this.orchestrator,
          schedulerService, Optional.of(LOGGER));
      this.serviceLauncher.addService(schedulerService);
      this.serviceLauncher.addService(this.scheduler);
    }

    // Initialize RestLI
    this.resourceHandler = new GobblinServiceFlowConfigResourceHandler(serviceName,
        this.flowCatalogLocalCommit,
        new FlowConfigResourceLocalHandler(this.flowCatalog),
        this.helixManager,
        this.scheduler);

    this.v2ResourceHandler = new GobblinServiceFlowConfigResourceHandler(serviceName,
        this.flowCatalogLocalCommit,
        new FlowConfigV2ResourceLocalHandler(this.flowCatalog),
        this.helixManager,
        this.scheduler);

    this.isRestLIServerEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_RESTLI_SERVER_ENABLED_KEY, true);

    if (isRestLIServerEnabled) {
      Injector injector = Guice.createInjector(new Module() {
        @Override
        public void configure(Binder binder) {
          binder.bind(FlowConfigsResourceHandler.class)
              .annotatedWith(Names.named(FlowConfigsResource.INJECT_FLOW_CONFIG_RESOURCE_HANDLER))
              .toInstance(GobblinServiceManager.this.resourceHandler);
          binder.bind(FlowConfigsResourceHandler.class)
              .annotatedWith(Names.named(FlowConfigsV2Resource.FLOW_CONFIG_GENERATOR_INJECT_NAME))
              .toInstance(GobblinServiceManager.this.v2ResourceHandler);
          binder.bindConstant()
              .annotatedWith(Names.named(FlowConfigsResource.INJECT_READY_TO_USE))
              .to(Boolean.TRUE);
          binder.bindConstant()
              .annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_READY_TO_USE))
              .to(Boolean.TRUE);
          binder.bind(RequesterService.class)
              .annotatedWith(Names.named(FlowConfigsResource.INJECT_REQUESTER_SERVICE))
              .toInstance(new NoopRequesterService(config));
          binder.bind(RequesterService.class)
              .annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_REQUESTER_SERVICE))
              .toInstance(new NoopRequesterService(config));
        }
      });
      this.restliServer = EmbeddedRestliServer.builder()
          .resources(Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsResource.class))
          .injector(injector)
          .build();
      if (config.hasPath(ServiceConfigKeys.SERVICE_PORT)) {
        this.restliServer.setPort(config.getInt(ServiceConfigKeys.SERVICE_PORT));
      }

      this.serviceLauncher.addService(restliServer);
    }

    // Register Scheduler to listen to changes in Flows
    if (isSchedulerEnabled) {
      this.flowCatalog.addListener(this.scheduler);
    }

    // Initialize TopologySpecFactory
    this.isTopologySpecFactoryEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_TOPOLOGY_SPEC_FACTORY_ENABLED_KEY, true);
    if (this.isTopologySpecFactoryEnabled) {
      this.aliasResolver = new ClassAliasResolver<>(TopologySpecFactory.class);
      String topologySpecFactoryClass = ServiceConfigKeys.DEFAULT_TOPOLOGY_SPEC_FACTORY;
      if (config.hasPath(ServiceConfigKeys.TOPOLOGYSPEC_FACTORY_KEY)) {
        topologySpecFactoryClass = config.getString(ServiceConfigKeys.TOPOLOGYSPEC_FACTORY_KEY);
      }

      try {
        LOGGER.info("Using TopologySpecFactory class name/alias " + topologySpecFactoryClass);
        this.topologySpecFactory = (TopologySpecFactory) ConstructorUtils
            .invokeConstructor(Class.forName(this.aliasResolver.resolve(topologySpecFactoryClass)), config);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
          | InstantiationException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  public boolean isLeader() {
    // If helix manager is absent, then this standalone instance hence leader
    // .. else check if this master of cluster
    return !helixManager.isPresent() || helixManager.get().isLeader();
  }

  /**
   * Build the {@link HelixManager} for the Service Master.
   */
  private HelixManager buildHelixManager(Config config, String zkConnectionString) {
    String helixClusterName = config.getString(ServiceConfigKeys.HELIX_CLUSTER_NAME_KEY);
    String helixInstanceName = ConfigUtils.getString(config, ServiceConfigKeys.HELIX_INSTANCE_NAME_KEY,
        GobblinServiceManager.class.getSimpleName());

    LOGGER.info("Creating Helix cluster if not already present [overwrite = false]: " + zkConnectionString);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, helixClusterName, false);

    return HelixUtils.buildHelixManager(helixInstanceName, helixClusterName, zkConnectionString);
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

  private FlowStatusGenerator buildFlowStatusGenerator(Config config) {
    JobStatusRetriever jobStatusRetriever;
    try {
      Class jobStatusRetrieverClass = Class.forName(ConfigUtils.getString(config, JOB_STATUS_RETRIEVER_CLASS_KEY, FsJobStatusRetriever.class.getName()));
      jobStatusRetriever =
          (JobStatusRetriever) GobblinConstructorUtils.invokeLongestConstructor(jobStatusRetrieverClass, config);
    } catch (ReflectiveOperationException e) {
      LOGGER.error("Exception encountered when instantiating JobStatusRetriever");
      throw new RuntimeException(e);
    }
    return FlowStatusGenerator.builder().jobStatusRetriever(jobStatusRetriever).build();
  }

  /**
   * Handle leadership change.
   * @param changeContext notification context
   */
  private void  handleLeadershipChange(NotificationContext changeContext) {
    if (this.helixManager.isPresent() && this.helixManager.get().isLeader()) {
      LOGGER.info("Leader notification for {} HM.isLeader {}", this.helixManager.get().getInstanceName(),
          this.helixManager.get().isLeader());

      if (this.isSchedulerEnabled) {
        LOGGER.info("Gobblin Service is now running in master instance mode, enabling Scheduler.");
        this.scheduler.setActive(true);
      }

      if (this.isGitConfigMonitorEnabled) {
        this.gitConfigMonitor.setActive(true);
      }

      if (this.isDagManagerEnabled) {
        //Activate DagManager only if TopologyCatalog is initialized. If not; skip activation.
        if (this.topologyCatalog.getInitComplete().getCount() == 0) {
          this.dagManager.setActive(true);
        }
      }
    } else if (this.helixManager.isPresent()) {
      LOGGER.info("Leader lost notification for {} HM.isLeader {}", this.helixManager.get().getInstanceName(),
          this.helixManager.get().isLeader());

      if (this.isSchedulerEnabled) {
        LOGGER.info("Gobblin Service is now running in slave instance mode, disabling Scheduler.");
        this.scheduler.setActive(false);
      }

      if (this.isGitConfigMonitorEnabled) {
        this.gitConfigMonitor.setActive(false);
      }

      if (this.isDagManagerEnabled) {
        this.dagManager.setActive(false);
      }
    }
  }

  @Override
  public void start() throws ApplicationException {
    LOGGER.info("[Init] Starting the Gobblin Service Manager");

    if (this.helixManager.isPresent()) {
      connectHelixManager();
    }

    this.eventBus.register(this);
    this.serviceLauncher.start();

    if (this.helixManager.isPresent()) {
      // Subscribe to leadership changes
      this.helixManager.get().addControllerListener(new ControllerChangeListener() {
        @Override
        public void onControllerChange(NotificationContext changeContext) {
          handleLeadershipChange(changeContext);
        }
      });

      // Update for first time since there might be no notification
      if (helixManager.get().isLeader()) {
        if (this.isSchedulerEnabled) {
          LOGGER.info("[Init] Gobblin Service is running in master instance mode, enabling Scheduler.");
          this.scheduler.setActive(true);
        }

        if (this.isGitConfigMonitorEnabled) {
          this.gitConfigMonitor.setActive(true);
        }

      } else {
        if (this.isSchedulerEnabled) {
          LOGGER.info("[Init] Gobblin Service is running in slave instance mode, not enabling Scheduler.");
        }
      }
    } else {
      // No Helix manager, hence standalone service instance
      // .. designate scheduler to itself
      LOGGER.info("[Init] Gobblin Service is running in single instance mode, enabling Scheduler.");
      this.scheduler.setActive(true);

      if (this.isGitConfigMonitorEnabled) {
        this.gitConfigMonitor.setActive(true);
      }
    }

    // Populate TopologyCatalog with all Topologies generated by TopologySpecFactory
    // This has to be done after the topologyCatalog service is launched
    if (this.isTopologySpecFactoryEnabled) {
      Collection<TopologySpec> topologySpecs = this.topologySpecFactory.getTopologies();
      for (TopologySpec topologySpec : topologySpecs) {
        this.topologyCatalog.put(topologySpec);
      }
    }

    // Register Orchestrator to listen to changes in topology
    // This has to be done after topologySpecFactory has updated spec store, so that listeners will have the latest updates.
    if (isSchedulerEnabled) {
      this.topologyCatalog.addListener(this.orchestrator);
    }

    // Notify now topologyCatalog has the right information
    this.topologyCatalog.getInitComplete().countDown();

    //Activate the SpecCompiler, after the topologyCatalog has been initialized.
    this.orchestrator.getSpecCompiler().setActive(true);

    //Activate the DagManager service, after the topologyCatalog has been initialized.
    if (!this.helixManager.isPresent() || this.helixManager.get().isLeader()){
      if (this.isDagManagerEnabled) {
        this.dagManager.setActive(true);
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
                new ControllerUserDefinedMessageHandlerFactory(flowCatalogLocalCommit, scheduler, resourceHandler, serviceName));
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
      try (GobblinServiceManager gobblinServiceManager = new GobblinServiceManager(
          cmd.getOptionValue(SERVICE_NAME_OPTION_NAME), getServiceId(cmd),
          config, Optional.<Path>absent())) {
        gobblinServiceManager.getOrchestrator().setFlowStatusGenerator(gobblinServiceManager.buildFlowStatusGenerator(config));
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
}
