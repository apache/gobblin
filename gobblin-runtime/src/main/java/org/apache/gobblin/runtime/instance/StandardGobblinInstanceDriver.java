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
package gobblin.runtime.instance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SharedResourcesBrokerImpl;
import gobblin.broker.SimpleScope;
import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.GobblinInstanceLauncher;
import gobblin.runtime.api.GobblinInstancePlugin;
import gobblin.runtime.api.GobblinInstancePluginFactory;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.runtime.job_catalog.ImmutableFSJobCatalog;
import gobblin.runtime.job_catalog.InMemoryJobCatalog;
import gobblin.runtime.job_exec.JobLauncherExecutionDriver;
import gobblin.runtime.plugins.email.EmailNotificationPlugin;
import gobblin.runtime.scheduler.ImmediateJobSpecScheduler;
import gobblin.runtime.scheduler.QuartzJobSpecScheduler;
import gobblin.runtime.std.DefaultConfigurableImpl;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;

/** A simple wrapper {@link DefaultGobblinInstanceDriverImpl} that will instantiate necessary
 * sub-components (e.g. {@link JobCatalog}, {@link JobSpecScheduler}, {@link JobExecutionLauncher}
 * and it will manage their lifecycle. */
public class StandardGobblinInstanceDriver extends DefaultGobblinInstanceDriverImpl {

  public static final String INSTANCE_CFG_PREFIX = "gobblin.instance";
  /** A comma-separated list of class names or aliases of {@link GobblinInstancePluginFactory} for
   * plugins to be instantiated with this instance. */
  public static final String PLUGINS_KEY = "plugins";
  public static final String PLUGINS_FULL_KEY = INSTANCE_CFG_PREFIX + "." + PLUGINS_KEY;

  private ServiceManager _subservices;
  private final List<GobblinInstancePlugin> _plugins;

  protected StandardGobblinInstanceDriver(String instanceName, Configurable sysConfig,
      JobCatalog jobCatalog,
      JobSpecScheduler jobScheduler, JobExecutionLauncher jobLauncher,
      Optional<MetricContext> instanceMetricContext,
      Optional<Logger> log,
      List<GobblinInstancePluginFactory> plugins,
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {
    super(instanceName, sysConfig, jobCatalog, jobScheduler, jobLauncher, instanceMetricContext, log, instanceBroker);
    List<Service> componentServices = new ArrayList<>();

    checkComponentService(getJobCatalog(), componentServices);
    checkComponentService(getJobScheduler(), componentServices);
    checkComponentService(getJobLauncher(), componentServices);

    _plugins = createPlugins(plugins, componentServices);

    if (componentServices.size() > 0) {
      _subservices = new ServiceManager(componentServices);
    }
  }

  private List<GobblinInstancePlugin> createPlugins(List<GobblinInstancePluginFactory> plugins,
      List<Service> componentServices) {
    List<GobblinInstancePlugin> res = new ArrayList<>();

    for (GobblinInstancePluginFactory pluginFactory: plugins) {
      Optional<GobblinInstancePlugin> plugin = createPlugin(this, pluginFactory, componentServices);
      if (plugin.isPresent()) {
        res.add(plugin.get());
      }
    }
    return res;
  }

  static Optional<GobblinInstancePlugin> createPlugin(StandardGobblinInstanceDriver instance,
      GobblinInstancePluginFactory pluginFactory, List<Service> componentServices) {
    instance.getLog().info("Instantiating a plugin of type: " + pluginFactory);
    try {
      GobblinInstancePlugin plugin = pluginFactory.createPlugin(instance);
      componentServices.add(plugin);
      instance.getLog().info("Instantiated plugin: " + plugin);
      return Optional.of(plugin);
    }
    catch (RuntimeException e) {
      instance.getLog().warn("Failed to create plugin: " + e, e);
    }
    return Optional.absent();
  }

  @Override
  protected void startUp() throws Exception {
    getLog().info("Starting driver ...");
    if (null != _subservices) {
      getLog().info("Starting subservices");
      _subservices.startAsync();
      _subservices.awaitHealthy(getInstanceCfg().getStartTimeoutMs(), TimeUnit.MILLISECONDS);
      getLog().info("All subservices have been started.");
    }
    else {
      getLog().info("No subservices found.");
    }
    super.startUp();
  }

  private void checkComponentService(Object component, List<Service> componentServices) {
    if (component instanceof Service) {
      componentServices.add((Service)component);
    }

  }

  @Override protected void shutDown() throws Exception {
    getLog().info("Shutting down driver ...");
    super.shutDown();
    if (null != _subservices) {
      getLog().info("Shutting down subservices ...");
      _subservices.stopAsync();
      _subservices.awaitStopped(getInstanceCfg().getShutdownTimeoutMs(), TimeUnit.MILLISECONDS);
      getLog().info("All subservices have been shutdown.");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for StandardGobblinInstanceDriver instances. The goal is to be convention driven
   * rather than configuration.
   *
   * <p>Conventions:
   * <ul>
   *  <li> Logger uses the instance name as a category
   *  <li> Default implementations of JobCatalog, JobSpecScheduler, JobExecutionLauncher use the
   *       logger as their logger.
   * </ul>
   *
   */
  public static class Builder implements GobblinInstanceEnvironment {
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);

    private Optional<GobblinInstanceEnvironment> _instanceEnv =
        Optional.<GobblinInstanceEnvironment>absent();
    private Optional<String> _instanceName = Optional.absent();
    private Optional<Logger> _log = Optional.absent();
    private Optional<JobCatalog> _jobCatalog = Optional.absent();
    private Optional<JobSpecScheduler> _jobScheduler = Optional.absent();
    private Optional<JobExecutionLauncher> _jobLauncher = Optional.absent();
    private Optional<MetricContext> _metricContext = Optional.absent();
    private Optional<Boolean> _instrumentationEnabled = Optional.absent();
    private Optional<SharedResourcesBroker<GobblinScopeTypes>> _instanceBroker = Optional.absent();
    private List<GobblinInstancePluginFactory> _plugins = new ArrayList<>();
    private final ClassAliasResolver<GobblinInstancePluginFactory> _aliasResolver =
        new ClassAliasResolver<>(GobblinInstancePluginFactory.class);

    public Builder(Optional<GobblinInstanceEnvironment> instanceLauncher) {
      _instanceEnv = instanceLauncher;
    }

    /** Constructor with no Gobblin instance launcher */
    public Builder() {
    }

    /** Constructor with a launcher */
    public Builder(GobblinInstanceLauncher instanceLauncher) {
      this();
      withInstanceEnvironment(instanceLauncher);
    }

    public Builder withInstanceEnvironment(GobblinInstanceEnvironment instanceLauncher) {
      Preconditions.checkNotNull(instanceLauncher);
      _instanceEnv = Optional.of(instanceLauncher);
      return this;
    }

    public Optional<GobblinInstanceEnvironment> getInstanceEnvironment() {
      return _instanceEnv;
    }

    public String getDefaultInstanceName() {
      if (_instanceEnv.isPresent()) {
        return _instanceEnv.get().getInstanceName();
      }
      else {
        return StandardGobblinInstanceDriver.class.getName() + "-" +
               INSTANCE_COUNTER.getAndIncrement();
      }
    }

    @Override
    public String getInstanceName() {
      if (! _instanceName.isPresent()) {
        _instanceName = Optional.of(getDefaultInstanceName());
      }
      return _instanceName.get();
    }

    public Builder withInstanceName(String instanceName) {
      _instanceName = Optional.of(instanceName);
      return this;
    }

    public Logger getDefaultLog() {
      return _instanceEnv.isPresent() ? _instanceEnv.get().getLog() :
            LoggerFactory.getLogger(getInstanceName());
    }

    @Override
    public Logger getLog() {
      if (! _log.isPresent()) {
        _log = Optional.of(getDefaultLog());
      }
      return _log.get();
    }

    public Builder withLog(Logger log) {
      _log = Optional.of(log);
      return this;
    }

    public JobCatalog getDefaultJobCatalog() {
      return new InMemoryJobCatalog(this);
    }

    public JobCatalog getJobCatalog() {
      if (! _jobCatalog.isPresent()) {
        _jobCatalog = Optional.of(getDefaultJobCatalog());
      }
      return _jobCatalog.get();
    }

    public Builder withJobCatalog(JobCatalog jobCatalog) {
      _jobCatalog = Optional.of(jobCatalog);
      return this;
    }

    public Builder withInMemoryJobCatalog() {
      return withJobCatalog(new InMemoryJobCatalog(this));
    }

    public Builder withFSJobCatalog() {
      try {
        return withJobCatalog(new FSJobCatalog(this));
      } catch (IOException e) {
        throw new RuntimeException("Unable to create FS Job Catalog: " + e, e);
      }
    }

    public Builder withImmutableFSJobCatalog() {
      try {
        return withJobCatalog(new ImmutableFSJobCatalog(this));
      } catch (IOException e) {
        throw new RuntimeException("Unable to create FS Job Catalog: " + e, e);
      }
    }

    public JobSpecScheduler getDefaultJobScheduler() {
      return new ImmediateJobSpecScheduler(Optional.of(getLog()));
    }

    public JobSpecScheduler getJobScheduler() {
      if (!_jobScheduler.isPresent()) {
        _jobScheduler = Optional.of(getDefaultJobScheduler());
      }
      return _jobScheduler.get();
    }

    public Builder withJobScheduler(JobSpecScheduler jobScheduler) {
      _jobScheduler = Optional.of(jobScheduler);
      return this;
    }

    public Builder withImmediateJobScheduler() {
      return withJobScheduler(new ImmediateJobSpecScheduler(Optional.of(getLog())));
    }

    public Builder withQuartzJobScheduler() {
      return withJobScheduler(new QuartzJobSpecScheduler(this));
    }

    public JobExecutionLauncher getDefaultJobLauncher() {
      JobLauncherExecutionDriver.Launcher res =
          new JobLauncherExecutionDriver.Launcher().withGobblinInstanceEnvironment(this);
      return res;
    }

    public JobExecutionLauncher getJobLauncher() {
      if (! _jobLauncher.isPresent()) {
        _jobLauncher = Optional.of(getDefaultJobLauncher());
      }
      return _jobLauncher.get();
    }

    public Builder withJobLauncher(JobExecutionLauncher jobLauncher) {
      _jobLauncher = Optional.of(jobLauncher);
      return this;
    }

    public Builder withMetricContext(MetricContext instanceMetricContext) {
      _metricContext = Optional.of(instanceMetricContext);
      return this;
    }

    @Override
    public MetricContext getMetricContext() {
      if (!_metricContext.isPresent()) {
        _metricContext = Optional.of(getDefaultMetricContext());
      }
      return _metricContext.get();
    }

    public MetricContext getDefaultMetricContext() {
      gobblin.configuration.State fakeState =
          new gobblin.configuration.State(getSysConfig().getConfigAsProperties());
      List<Tag<?>> tags = new ArrayList<>();
      tags.add(new Tag<>(StandardMetrics.INSTANCE_NAME_TAG, getInstanceName()));
      MetricContext res = Instrumented.getMetricContext(fakeState,
          StandardGobblinInstanceDriver.class, tags);
      return res;
    }

    public Builder withInstanceBroker(SharedResourcesBroker<GobblinScopeTypes> broker) {
      _instanceBroker = Optional.of(broker);
      return this;
    }

    @Override
    public SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker() {
      if (!_instanceBroker.isPresent()) {
        _instanceBroker = Optional.of(getDefaultInstanceBroker());
      }
      return _instanceBroker.get();
    }

    public SharedResourcesBroker<GobblinScopeTypes> getDefaultInstanceBroker() {
      SharedResourcesBrokerImpl<GobblinScopeTypes> globalBroker =
          SharedResourcesBrokerFactory.createDefaultTopLevelBroker(getSysConfig().getConfig(),
              GobblinScopeTypes.GLOBAL.defaultScopeInstance());
      return globalBroker.newSubscopedBuilder(new SimpleScope<>(GobblinScopeTypes.INSTANCE, getInstanceName())).build();
    }

      public StandardGobblinInstanceDriver build() {
      Configurable sysConfig = getSysConfig();
      return new StandardGobblinInstanceDriver(getInstanceName(), sysConfig, getJobCatalog(),
             getJobScheduler(),
             getJobLauncher(),
             isInstrumentationEnabled() ? Optional.of(getMetricContext()) :
                   Optional.<MetricContext>absent(),
             Optional.of(getLog()),
             getPlugins(),
             getInstanceBroker()
             );
    }

    @Override public Configurable getSysConfig() {
      return _instanceEnv.isPresent() ? _instanceEnv.get().getSysConfig() :
          DefaultConfigurableImpl.createFromConfig(ConfigFactory.load());
    }

    public Builder withInstrumentationEnabled(boolean enabled) {
      _instrumentationEnabled = Optional.of(enabled);
      return this;
    }

    public boolean getDefaultInstrumentationEnabled() {
      return GobblinMetrics.isEnabled(getSysConfig().getConfig());
    }

    @Override
    public boolean isInstrumentationEnabled() {
      if (!_instrumentationEnabled.isPresent()) {
        _instrumentationEnabled = Optional.of(getDefaultInstrumentationEnabled());
      }
      return _instrumentationEnabled.get();
    }

    @Override public List<Tag<?>> generateTags(gobblin.configuration.State state) {
      return Collections.emptyList();
    }

    @Override public void switchMetricContext(List<Tag<?>> tags) {
      throw new UnsupportedOperationException();
    }

    @Override public void switchMetricContext(MetricContext context) {
      throw new UnsupportedOperationException();
    }

    /**
     * Returns the list of plugins as defined in the system configuration. These are the
     * defined in the PLUGINS_FULL_KEY config option.
     * The list also includes plugins that are automatically added by gobblin.
     * */
    public List<GobblinInstancePluginFactory> getDefaultPlugins() {

      List<String> pluginNames =
          ConfigUtils.getStringList(getSysConfig().getConfig(), PLUGINS_FULL_KEY);

      List<GobblinInstancePluginFactory> pluginFactories = Lists.newArrayList();

      // By default email notification plugin is added.
      if (!ConfigUtils.getBoolean(getSysConfig().getConfig(), EmailNotificationPlugin.EMAIL_NOTIFICATIONS_DISABLED_KEY,
          EmailNotificationPlugin.EMAIL_NOTIFICATIONS_DISABLED_DEFAULT)) {
        pluginFactories.add(new EmailNotificationPlugin.Factory());
      }

      pluginFactories.addAll(Lists.transform(pluginNames, new Function<String, GobblinInstancePluginFactory>() {

        @Override public GobblinInstancePluginFactory apply(String input) {
          Class<? extends GobblinInstancePluginFactory> factoryClass;
          try {
            factoryClass = _aliasResolver.resolveClass(input);
            return factoryClass.newInstance();
          } catch (ClassNotFoundException|InstantiationException|IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate plugin factory " + input + ": " + e, e);
          }
        }
      }));

      return pluginFactories;
    }

    public List<GobblinInstancePluginFactory> getPlugins() {
      List<GobblinInstancePluginFactory> res = new ArrayList<>(getDefaultPlugins());
      res.addAll(_plugins);
      return res;
    }

    public Builder addPlugin(GobblinInstancePluginFactory pluginFactory) {
      _plugins.add(pluginFactory);
      return this;
    }
  }

  public List<GobblinInstancePlugin> getPlugins() {
    return _plugins;
  }
}
