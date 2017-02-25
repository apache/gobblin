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

package gobblin.service.modules.core;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.app.ApplicationException;
import gobblin.runtime.app.ApplicationLauncher;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.runtime.spec_catalog.FlowCatalog;
import gobblin.service.FlowConfig;
import gobblin.service.FlowConfigClient;
import gobblin.service.FlowConfigsResource;
import gobblin.service.ServiceConfigKeys;
import gobblin.service.modules.orchestration.Orchestrator;
import gobblin.service.modules.topology.TopologySpecFactory;
import gobblin.runtime.spec_catalog.TopologyCatalog;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;


public class GobblinServiceManager implements ApplicationLauncher {

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
  protected final boolean isOrchestratorEnabled;
  protected final boolean isRestLIServerEnabled;
  protected final boolean isTopologySpecFactoryEnabled;

  protected TopologyCatalog topologyCatalog;
  @Getter
  protected FlowCatalog flowCatalog;
  protected Orchestrator orchestrator;
  protected EmbeddedRestliServer restliServer;
  protected TopologySpecFactory topologySpecFactory;

  protected ClassAliasResolver<TopologySpecFactory> aliasResolver;

  public GobblinServiceManager(String serviceName, String serviceId, Config config,
      Optional<Path> serviceWorkDirOptional) throws Exception {

    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    Properties properties = ConfigUtils.configToProperties(config);
    if (!properties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      properties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }

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
    }

    // Initialize Orchestrator
    this.isOrchestratorEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.GOBBLIN_SERVICE_ORCHESTRATOR_ENABLED_KEY, true);
    if (isOrchestratorEnabled) {
      this.orchestrator = new Orchestrator(config, Optional.of(this.flowCatalog), Optional.of(this.topologyCatalog),
          Optional.of(LOGGER));
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

    // Register Orchestrator to listen to changes in Topologies and Flows
    if (isOrchestratorEnabled) {
      topologyCatalog.addListener(orchestrator);
      flowCatalog.addListener(orchestrator);
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

  private FileSystem buildFileSystem(Config config)
      throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), new Configuration())
        : FileSystem.get(new Configuration());
  }

  private Path getServiceWorkDirPath(FileSystem fs, String serviceName, String serviceId) {
    return new Path(fs.getHomeDirectory(), serviceName + Path.SEPARATOR + serviceId);
  }

  @Override
  public void start() throws ApplicationException {
    LOGGER.info("Starting the Gobblin Service Manager");

    this.eventBus.register(this);
    this.serviceLauncher.start();

    // Populate TopologyCatalog with all Topologies generated by TopologySpecFactory
    if (this.isTopologySpecFactoryEnabled) {
      Collection<TopologySpec> topologySpecs = this.topologySpecFactory.getTopologies();
      for (TopologySpec topologySpec : topologySpecs) {
        this.topologyCatalog.put(topologySpec);
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
    }
  }

  @Override
  public void close() throws IOException {
    this.serviceLauncher.close();
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
    final String TEST_SCHEDULE = "";
    final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setRunImmediately(true)
        .setProperties(new StringMap(flowProperties));

    try {
      client.createFlowConfig(flowConfig);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }
}
