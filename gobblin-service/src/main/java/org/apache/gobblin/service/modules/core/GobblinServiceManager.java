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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.Schedule;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import lombok.Getter;

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
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigClient;
import org.apache.gobblin.service.FlowConfigsResource;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.topology.TopologySpecFactory;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
public class GobblinServiceManager implements ApplicationLauncher, StandardMetricsBridge{

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceManager.class);

  protected final ServiceBasedAppLauncher serviceLauncher;
  private volatile boolean stopInProgress = false;

  // An EventBus used for communications between services running in the ApplicationMaster
  protected final EventBus eventBus = new EventBus(GobblinServiceManager.class.getSimpleName());

  protected final FileSystem fs;
  protected final Path serviceWorkDir;

  protected final String serviceId;

  protected final boolean isTopologyCatalogEnabled;
  protected final boolean isFlowCatalogEnabled;
  protected final boolean isSchedulerEnabled;
  protected final boolean isRestLIServerEnabled;
  protected final boolean isTopologySpecFactoryEnabled;
  protected final boolean isGitConfigMonitorEnabled;

  protected TopologyCatalog topologyCatalog;
  @Getter
  protected FlowCatalog flowCatalog;
  @Getter
  protected GobblinServiceJobScheduler scheduler;
  protected Orchestrator orchestrator;
  protected EmbeddedRestliServer restliServer;
  protected TopologySpecFactory topologySpecFactory;

  protected Optional<HelixManager> helixManager;

  protected ClassAliasResolver<TopologySpecFactory> aliasResolver;

  protected GitConfigMonitor gitConfigMonitor;

  @Getter
  protected Config config;

  private final MetricContext metricContext;
  private final Metrics metrics;

  public GobblinServiceManager(String serviceName, String serviceId, Config config,
      Optional<Path> serviceWorkDirOptional) throws Exception {

    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    Properties properties = ConfigUtils.configToProperties(config);
    if (!properties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      properties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }
    this.config = config;
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.getClass());
    this.metrics = new Metrics(this.metricContext);
    this.serviceId = serviceId;
    this.serviceLauncher = new ServiceBasedAppLauncher(properties, serviceName);

    this.fs = buildFileSystem(config);
    this.serviceWorkDir = serviceWorkDirOptional.isPresent() ? serviceWorkDirOptional.get() :
        getServiceWorkDirPath(this.fs, serviceName, serviceId);

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

    // Initialize ServiceScheduler
    this.isSchedulerEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_SCHEDULER_ENABLED_KEY, true);
    if (isSchedulerEnabled) {
      this.orchestrator = new Orchestrator(config, Optional.of(this.topologyCatalog), Optional.of(LOGGER));
      SchedulerService schedulerService = new SchedulerService(properties);
      this.scheduler = new GobblinServiceJobScheduler(config, this.helixManager,
          Optional.of(this.flowCatalog), Optional.of(this.topologyCatalog), this.orchestrator,
          schedulerService, Optional.of(LOGGER));
      this.serviceLauncher.addService(schedulerService);
      this.serviceLauncher.addService(this.scheduler);
    }

    // Initialize RestLI
    this.isRestLIServerEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_RESTLI_SERVER_ENABLED_KEY, true);
    if (isRestLIServerEnabled) {
      Injector injector = Guice.createInjector(new Module() {
        @Override
        public void configure(Binder binder) {
          binder.bind(FlowCatalog.class).annotatedWith(Names.named("flowCatalog")).toInstance(flowCatalog);
          binder.bindConstant().annotatedWith(Names.named("readyToUse")).to(Boolean.TRUE);
        }
      });
      this.restliServer = EmbeddedRestliServer.builder()
          .resources(Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsResource.class))
          .injector(injector)
          .build();
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

  /**
   * Handle leadership change.
   * @param changeContext notification context
   */
  private void handleLeadershipChange(NotificationContext changeContext) {
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
                getUserDefinedMessageHandlerFactory());
      }
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates and returns a {@link MessageHandlerFactory} for handling of Helix
   * {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}s.
   *
   * @returns a {@link MessageHandlerFactory}.
   */
  protected MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new ControllerUserDefinedMessageHandlerFactory(this.flowCatalog, this.scheduler);
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
  public StandardMetrics getStandardMetrics() {
    return this.metrics;
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return false;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return null;
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  private class Metrics extends StandardMetrics {
    public static final String SERVICE_LEADERSHIP_CHANGE = "serviceLeadershipChange";
    private ContextAwareHistogram serviceLeadershipChange;
    public Metrics(final MetricContext metricContext) {
      serviceLeadershipChange = metricContext.contextAwareHistogram(SERVICE_LEADERSHIP_CHANGE, 1, TimeUnit.MINUTES);
    }

    @Override
    public Collection<ContextAwareHistogram> getHistograms() {
      return ImmutableList.of(this.serviceLeadershipChange);
    }
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ControllerUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private static class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    private FlowCatalog flowCatalog;
    private GobblinServiceJobScheduler jobScheduler;

    public ControllerUserDefinedMessageHandlerFactory(FlowCatalog flowCatalog, GobblinServiceJobScheduler jobScheduler) {
      this.flowCatalog = flowCatalog;
      this.jobScheduler = jobScheduler;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerUserDefinedMessageHandler(flowCatalog, jobScheduler, message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.USER_DEFINE_MSG.toString();
    }

    public List<String> getMessageTypes() {
      return Collections.singletonList(getMessageType());
    }

    @Override
    public void reset() {

    }

    /**
     * A custom {@link MessageHandler} for handling user-defined messages to the controller.
     */
    private static class ControllerUserDefinedMessageHandler extends MessageHandler {

      private FlowCatalog flowCatalog;
      private GobblinServiceJobScheduler jobScheduler;

      public ControllerUserDefinedMessageHandler(FlowCatalog flowCatalog, GobblinServiceJobScheduler jobScheduler,
          Message message, NotificationContext context) {
        super(message, context);

        this.flowCatalog = flowCatalog;
        this.jobScheduler = jobScheduler;
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        if (jobScheduler.isActive()) {
          String flowSpecUri = _message.getAttribute(Message.Attributes.INNER_MESSAGE);
          LOGGER.info ("ControllerUserDefinedMessage received : {}, type {}", flowSpecUri, _message.getMsgSubType());
          try {
            if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_ADD)) {
              Spec spec = flowCatalog.getSpec(new URI(flowSpecUri));
              this.jobScheduler.onAddSpec(spec);
            } else if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_REMOVE)) {
              List<String> flowSpecUriParts = Splitter.on(":").omitEmptyStrings().trimResults().splitToList(flowSpecUri);
              this.jobScheduler.onDeleteSpec(new URI(flowSpecUriParts.get(0)), flowSpecUriParts.get(1));
            } else if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_UPDATE)) {
              Spec spec = flowCatalog.getSpec(new URI(flowSpecUri));
              this.jobScheduler.onUpdateSpec(spec);
            }
          } catch (SpecNotFoundException | URISyntaxException e) {
            LOGGER.error("Cannot process Helix message for flowSpecUri: " + flowSpecUri, e);
          }
        } else {
          String flowSpecUri = _message.getAttribute(Message.Attributes.INNER_MESSAGE);
          LOGGER.info ("ControllerUserDefinedMessage received but ignored due to not in active mode: {}, type {}", flowSpecUri, _message.getMsgSubType());
        }
        HelixTaskResult helixTaskResult = new HelixTaskResult();
        helixTaskResult.setSuccess(true);

        return helixTaskResult;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  private static String getServiceId() {
    return "1";
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", ServiceConfigKeys.SERVICE_NAME_OPTION_NAME, true, "Gobblin application name");
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
      if (!cmd.hasOption(ServiceConfigKeys.SERVICE_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      boolean isTestMode = false;
      if (cmd.hasOption("test_mode")) {
        isTestMode = Boolean.parseBoolean(cmd.getOptionValue("test_mode", "false"));
      }

      Config config = ConfigFactory.load();
      try (GobblinServiceManager gobblinServiceManager = new GobblinServiceManager(
          cmd.getOptionValue(ServiceConfigKeys.SERVICE_NAME_OPTION_NAME), getServiceId(),
          config, Optional.<Path>absent())) {

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
