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

import java.util.Objects;

import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import com.typesafe.config.Config;

import javax.inject.Singleton;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.instance.StandardGobblinInstanceLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.runtime.troubleshooter.InMemoryMultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.JobIssueEventHandler;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigV2ResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigsResource;
import org.apache.gobblin.service.FlowConfigsResourceHandler;
import org.apache.gobblin.service.FlowConfigsV2Resource;
import org.apache.gobblin.service.FlowConfigsV2ResourceHandler;
import org.apache.gobblin.service.FlowExecutionResource;
import org.apache.gobblin.service.FlowExecutionResourceHandler;
import org.apache.gobblin.service.FlowExecutionResourceLocalHandler;
import org.apache.gobblin.service.FlowStatusResource;
import org.apache.gobblin.service.GroupOwnershipService;
import org.apache.gobblin.service.NoopRequesterService;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.db.ServiceDatabaseManager;
import org.apache.gobblin.service.modules.db.ServiceDatabaseProvider;
import org.apache.gobblin.service.modules.db.ServiceDatabaseProviderImpl;
import org.apache.gobblin.service.modules.orchestration.DagActionProcessingMultiActiveLeaseArbiterFactory;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementTaskStreamImpl;
import org.apache.gobblin.service.modules.orchestration.DagProcFactory;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTaskStream;
import org.apache.gobblin.service.modules.orchestration.FlowLaunchHandler;
import org.apache.gobblin.service.modules.orchestration.FlowLaunchMultiActiveLeaseArbiterFactory;
import org.apache.gobblin.service.modules.orchestration.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MysqlDagActionStore;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.orchestration.proc.DagProcUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.restli.GobblinServiceFlowConfigResourceHandler;
import org.apache.gobblin.service.modules.restli.GobblinServiceFlowConfigV2ResourceHandlerWithWarmStandby;
import org.apache.gobblin.service.modules.restli.GobblinServiceFlowExecutionResourceHandlerWithWarmStandby;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.topology.TopologySpecFactory;
import org.apache.gobblin.service.modules.troubleshooter.MySqlMultiContextIssueRepository;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.modules.utils.SharedFlowMetricsSingleton;
import org.apache.gobblin.service.monitoring.DagManagementDagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.DagManagementDagActionStoreChangeMonitorFactory;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.service.monitoring.GitConfigMonitor;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitorFactory;
import org.apache.gobblin.service.monitoring.SpecStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.SpecStoreChangeMonitorFactory;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


public class GobblinServiceGuiceModule implements Module {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceGuiceModule.class);
  private static final String JOB_STATUS_RETRIEVER_CLASS_KEY = "jobStatusRetriever.class";

  GobblinServiceConfiguration serviceConfig;

  public GobblinServiceGuiceModule(GobblinServiceConfiguration serviceConfig) {
    this.serviceConfig = Objects.requireNonNull(serviceConfig);
  }

  @Override
  public void configure(Binder binder) {
    LOGGER.info("Configuring bindings for the following service settings: {}", serviceConfig);

    // In the current code base, we frequently inject classes instead of interfaces
    // As a result, even when the binding is missing, Guice will create an instance of the
    // the class and inject it. This interferes with disabling of different services and
    // components, because without explicit bindings they will get instantiated anyway.
    binder.requireExplicitBindings();

    // Optional binder will find the existing binding for T and create additional binding for Optional<T>.
    // If none of the specific class binding exist, optional will be "absent".
    OptionalBinder.newOptionalBinder(binder, Logger.class);

    binder.bind(Logger.class).toInstance(LoggerFactory.getLogger(GobblinServiceManager.class));

    binder.bind(Config.class).toInstance(serviceConfig.getInnerConfig());

    binder.bind(GobblinServiceConfiguration.class).toInstance(serviceConfig);

    // Used by TopologyCatalog and FlowCatalog
    GobblinInstanceEnvironment gobblinInstanceEnvironment = StandardGobblinInstanceLauncher.builder()
        .withLog(LoggerFactory.getLogger(GobblinServiceManager.class))
        .setInstrumentationEnabled(true)
        .withSysConfig(serviceConfig.getInnerConfig())
        .build();

    binder.bind(GobblinInstanceEnvironment.class).toInstance(gobblinInstanceEnvironment);

    binder.bind(EventBus.class)
        .annotatedWith(Names.named(GobblinServiceManager.SERVICE_EVENT_BUS_NAME))
        .toInstance(new EventBus(GobblinServiceManager.class.getSimpleName()));

    binder.bindConstant().annotatedWith(Names.named(InjectionNames.SERVICE_NAME)).to(serviceConfig.getServiceName());

    binder.bindConstant()
        .annotatedWith(Names.named(InjectionNames.FORCE_LEADER))
        .to(ConfigUtils.getBoolean(serviceConfig.getInnerConfig(), ServiceConfigKeys.FORCE_LEADER,
            ServiceConfigKeys.DEFAULT_FORCE_LEADER));

    binder.bindConstant()
        .annotatedWith(Names.named(InjectionNames.FLOW_CATALOG_LOCAL_COMMIT))
        .to(serviceConfig.isFlowCatalogLocalCommit());
    binder.bind(FlowConfigsResourceHandler.class).to(GobblinServiceFlowConfigResourceHandler.class);
    binder.bind(FlowConfigsV2ResourceHandler.class).to(GobblinServiceFlowConfigV2ResourceHandlerWithWarmStandby.class);
    binder.bind(FlowExecutionResourceHandler.class).to(GobblinServiceFlowExecutionResourceHandlerWithWarmStandby.class);

    /* Note that two instances of the same class can only be differentiated with an `annotatedWith` marker provided at
    binding time (optionally bound classes cannot have names associated with them), so both arbiters need to be
    explicitly bound to be differentiated. The scheduler lease arbiter is only used in single-active scheduler mode,
    while the execution lease arbiter is used in single-active or multi-active execution. */
    binder.bind(MultiActiveLeaseArbiter.class).annotatedWith(Names.named(
        ConfigurationKeys.SCHEDULER_LEASE_ARBITER_NAME)).toProvider(
            FlowLaunchMultiActiveLeaseArbiterFactory.class);
    binder.bind(MultiActiveLeaseArbiter.class).annotatedWith(Names.named(
        ConfigurationKeys.PROCESSING_LEASE_ARBITER_NAME)).toProvider(
            DagActionProcessingMultiActiveLeaseArbiterFactory.class);

    binder.bind(DagActionReminderScheduler.class);
    binder.bind(DagActionStore.class).to(MysqlDagActionStore.class);
    binder.bind(DagManagementDagActionStoreChangeMonitor.class).toProvider(
        DagManagementDagActionStoreChangeMonitorFactory.class).in(Singleton.class);
    binder.bind(DagManagement.class).to(DagManagementTaskStreamImpl.class);
    binder.bind(DagManagementStateStore.class).to(MySqlDagManagementStateStore.class);
    binder.bind(DagTaskStream.class).to(DagManagementTaskStreamImpl.class);
    binder.bind(DagProcFactory.class);
    binder.bind(DagProcessingEngine.class);
    binder.bind(DagProcessingEngineMetrics.class);
    binder.bind(FlowLaunchHandler.class);
    binder.bind(SpecStoreChangeMonitor.class).toProvider(SpecStoreChangeMonitorFactory.class).in(Singleton.class);
    binder.bind(FlowConfigsResource.class);
    binder.bind(FlowConfigsV2Resource.class);
    binder.bind(FlowStatusResource.class);
    binder.bind(FlowExecutionResource.class);

    binder.bind(FlowConfigResourceLocalHandler.class);
    binder.bind(FlowConfigV2ResourceLocalHandler.class);
    binder.bind(FlowExecutionResourceLocalHandler.class);

    binder.bindConstant().annotatedWith(Names.named(FlowConfigsResource.INJECT_READY_TO_USE)).to(Boolean.TRUE);
    binder.bindConstant().annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_READY_TO_USE)).to(Boolean.TRUE);
    binder.bind(RequesterService.class)
        .to(NoopRequesterService.class);

    binder.bind(SharedFlowMetricsSingleton.class);
    binder.bind(FlowCompilationValidationHelper.class);
    binder.bind(TopologyCatalog.class);

    if (serviceConfig.isTopologySpecFactoryEnabled()) {
      binder.bind(TopologySpecFactory.class)
          .to(getClassByNameOrAlias(TopologySpecFactory.class, serviceConfig.getInnerConfig(),
              ServiceConfigKeys.TOPOLOGYSPEC_FACTORY_KEY, ServiceConfigKeys.DEFAULT_TOPOLOGY_SPEC_FACTORY));
    }

    OptionalBinder.newOptionalBinder(binder, HelixManager.class);
    if (serviceConfig.isHelixManagerEnabled()) {
      binder.bind(HelixManager.class)
          .toInstance(buildHelixManager(serviceConfig.getInnerConfig(),
              serviceConfig.getInnerConfig().getString(ServiceConfigKeys.ZK_CONNECTION_STRING_KEY)));
    } else {
      LOGGER.info("No ZooKeeper connection string. Running in single instance mode.");
    }

    OptionalBinder.newOptionalBinder(binder, FlowCatalog.class);
    if (serviceConfig.isFlowCatalogEnabled()) {
      binder.bind(FlowCatalog.class);
    }

    if (serviceConfig.isJobStatusMonitorEnabled()) {
      binder.bind(KafkaJobStatusMonitor.class).toProvider(KafkaJobStatusMonitorFactory.class).in(Singleton.class);
    }

    binder.bind(FlowStatusGenerator.class);

    if (serviceConfig.isSchedulerEnabled()) {
      binder.bind(Orchestrator.class);
      binder.bind(SchedulerService.class);
      binder.bind(GobblinServiceJobScheduler.class);
      binder.bind(UserQuotaManager.class)
          .to(getClassByNameOrAlias(UserQuotaManager.class, serviceConfig.getInnerConfig(),
              ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER));
    }

    if (serviceConfig.isGitConfigMonitorEnabled()) {
      binder.bind(GitConfigMonitor.class);
    }

    binder.bind(GroupOwnershipService.class)
        .to(getClassByNameOrAlias(GroupOwnershipService.class, serviceConfig.getInnerConfig(),
            ServiceConfigKeys.GROUP_OWNERSHIP_SERVICE_CLASS, ServiceConfigKeys.DEFAULT_GROUP_OWNERSHIP_SERVICE));

    binder.bind(JobStatusRetriever.class)
        .to(getClassByNameOrAlias(JobStatusRetriever.class, serviceConfig.getInnerConfig(),
            JOB_STATUS_RETRIEVER_CLASS_KEY, FsJobStatusRetriever.class.getName()));

    if (serviceConfig.isRestLIServerEnabled()) {
      binder.bind(EmbeddedRestliServer.class).toProvider(EmbeddedRestliServerProvider.class);
    }

    binder.bind(GobblinServiceManager.class);

    binder.bind(ServiceDatabaseProvider.class).to(ServiceDatabaseProviderImpl.class);
    binder.bind(ServiceDatabaseProviderImpl.Configuration.class);
    binder.bind(ServiceDatabaseManager.class);

    binder.bind(MultiContextIssueRepository.class)
        .to(getClassByNameOrAlias(MultiContextIssueRepository.class, serviceConfig.getInnerConfig(),
                                  ServiceConfigKeys.ISSUE_REPO_CLASS,
                                  InMemoryMultiContextIssueRepository.class.getName()));
    binder.bind(MySqlMultiContextIssueRepository.Configuration.class);
    binder.bind(InMemoryMultiContextIssueRepository.Configuration.class);
    binder.bind(JobIssueEventHandler.class);

    binder.bind(D2Announcer.class).to(NoopD2Announcer.class);

    binder.bindConstant().annotatedWith(Names.named(DagProcessingEngine.DEFAULT_JOB_START_DEADLINE_TIME_MS)).to(
        DagProcUtils.getDefaultJobStartDeadline(serviceConfig.getInnerConfig()));

    LOGGER.info("Bindings configured");
  }

  protected HelixManager buildHelixManager(Config config, String zkConnectionString) {
    String helixClusterName = config.getString(ServiceConfigKeys.HELIX_CLUSTER_NAME_KEY);
    String helixInstanceName = HelixUtils.buildHelixInstanceName(config, GobblinServiceManager.class.getSimpleName());

    LOGGER.info(
        "Creating Helix cluster if not already present [overwrite = false]: " + zkConnectionString);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, helixClusterName, false);

    return HelixUtils.buildHelixManager(helixInstanceName, helixClusterName, zkConnectionString);
  }

  protected static <T> Class<? extends T> getClassByNameOrAlias(Class<T> baseClass, Config config,
      String classPropertyName, String defaultClass) {
    String className = ConfigUtils.getString(config, classPropertyName, defaultClass);
    ClassAliasResolver<T> aliasResolver = new ClassAliasResolver<T>(baseClass);
    try {
      return (Class<? extends T>) Class.forName(aliasResolver.resolve(className));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Cannot resolve the class '" + className + "'. Check that property '" + classPropertyName
              + "' points to a valid class name or alias.", e);
    }
  }

  public static class EmbeddedRestliServerProvider implements Provider<EmbeddedRestliServer> {
    Injector injector;

    @Inject
    public EmbeddedRestliServerProvider(Injector injector) {
      this.injector = injector;
    }

    @Override
    public EmbeddedRestliServer get() {
      return EmbeddedRestliServer.builder()
          .resources(Lists.newArrayList(FlowConfigsV2Resource.class, FlowConfigsV2Resource.class))
          .injector(injector)
          .build();
    }
  }
}
