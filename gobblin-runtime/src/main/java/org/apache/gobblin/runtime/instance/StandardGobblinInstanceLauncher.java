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
package org.apache.gobblin.runtime.instance;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.SimpleScope;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SharedResourcesBrokerImpl;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.Configurable;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.GobblinInstanceLauncher;
import org.apache.gobblin.runtime.std.DefaultConfigurableImpl;

/**
 * A standard implementation that expects the instance configuration to be passed from the outside.
 * As a driver, it uses {@link StandardGobblinInstanceDriver}.
 */
public class StandardGobblinInstanceLauncher extends AbstractIdleService
      implements GobblinInstanceLauncher {
  private final Logger _log;
  private final String _name;
  private final Configurable _instanceConf;
  private final StandardGobblinInstanceDriver _driver;
  private final MetricContext _metricContext;
  private final boolean _instrumentationEnabled;
  private final SharedResourcesBroker<GobblinScopeTypes> _instanceBroker;

  protected StandardGobblinInstanceLauncher(String name,
      Configurable instanceConf,
      StandardGobblinInstanceDriver.Builder driverBuilder,
      Optional<MetricContext> metricContext,
      Optional<Logger> log,
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {
    _log = log.or(LoggerFactory.getLogger(getClass()));
    _name = name;
    _instanceConf = instanceConf;
    _driver = driverBuilder.withInstanceEnvironment(this).build();
    _instrumentationEnabled = metricContext.isPresent();
    _metricContext = metricContext.orNull();
    _instanceBroker = instanceBroker;
  }

  /** {@inheritDoc} */
  @Override
  public Config getConfig() {
    return _instanceConf.getConfig();
  }

  /** {@inheritDoc} */
  @Override
  public Properties getConfigAsProperties() {
    return _instanceConf.getConfigAsProperties();
  }

  /** {@inheritDoc} */
  @Override
  public GobblinInstanceDriver getDriver() throws IllegalStateException {
    return _driver;
  }

  /** {@inheritDoc} */
  @Override
  public String getInstanceName() {
    return _name;
  }

  /** {@inheritDoc} */
  @Override
  public SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker() {
    return _instanceBroker;
  }

  /** {@inheritDoc} */
  @Override
  protected void startUp() throws Exception {
    _driver.startUp();
  }

  /** {@inheritDoc} */
  @Override
  protected void shutDown() throws Exception {
    _driver.shutDown();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements GobblinInstanceEnvironment {
    final static AtomicInteger INSTANCE_COUNT = new AtomicInteger(1);

    Optional<String> _name = Optional.absent();
    Optional<Logger> _log = Optional.absent();
    StandardGobblinInstanceDriver.Builder _driver = new StandardGobblinInstanceDriver.Builder();
    Optional<? extends Configurable> _instanceConfig = Optional.absent();
    Optional<Boolean> _instrumentationEnabled = Optional.absent();
    Optional<MetricContext> _metricContext = Optional.absent();
    Optional<SharedResourcesBroker<GobblinScopeTypes>> _instanceBroker = Optional.absent();

    public Builder() {
      _driver.withInstanceEnvironment(this);
    }

    public String getDefaultInstanceName() {
      return StandardGobblinInstanceLauncher.class.getSimpleName() + "-" +
             INSTANCE_COUNT.getAndIncrement();
    }

    @Override
    public String getInstanceName() {
      if (! _name.isPresent()) {
        _name = Optional.of(getDefaultInstanceName());
      }
      return _name.get();
    }

    public Builder withInstanceName(String instanceName) {
      _name = Optional.of(instanceName);
      return this;
    }

    public Logger getDefaultLog() {
      return LoggerFactory.getLogger(getInstanceName());
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

    public StandardGobblinInstanceDriver.Builder driver() {
      return _driver;
    }

    /**  Uses the configuration provided by {@link ConfigFactory#load()} */
    public Configurable getDefaultSysConfig() {
      return DefaultConfigurableImpl.createFromConfig(ConfigFactory.load());
    }

    @Override public Configurable getSysConfig() {
      if (! _instanceConfig.isPresent()) {
        _instanceConfig = Optional.of(getDefaultSysConfig());
      }
      return _instanceConfig.get();
    }

    public Builder withSysConfig(Config instanceConfig) {
      _instanceConfig = Optional.of(DefaultConfigurableImpl.createFromConfig(instanceConfig));
      return this;
    }

    public Builder withSysConfig(Properties instanceConfig) {
      _instanceConfig = Optional.of(DefaultConfigurableImpl.createFromProperties(instanceConfig));
      return this;
    }

    public Builder setInstrumentationEnabled(boolean enabled) {
      _instrumentationEnabled = Optional.of(enabled);
      return this;
    }

    @Override
    public boolean isInstrumentationEnabled() {
      if (!_instrumentationEnabled.isPresent()) {
        _instrumentationEnabled = Optional.of(getDefaultInstrumentationEnabled());
      }
      return _instrumentationEnabled.get();
    }

    public Builder setMetricContext(MetricContext metricContext) {
      _metricContext = Optional.of(metricContext);
      return this;
    }

    @Override
    public MetricContext getMetricContext() {
      if (!_metricContext.isPresent()) {
        _metricContext = Optional.of(getDefaultMetricContext());
      }
      return _metricContext.get();
    }

    private MetricContext getDefaultMetricContext() {
      org.apache.gobblin.configuration.State fakeState =
          new org.apache.gobblin.configuration.State(getSysConfig().getConfigAsProperties());
      return Instrumented.getMetricContext(fakeState, StandardGobblinInstanceLauncher.class);
    }

    private boolean getDefaultInstrumentationEnabled() {
      return GobblinMetrics.isEnabled(getSysConfig().getConfig());
    }

    public StandardGobblinInstanceLauncher.Builder withInstanceBroker(SharedResourcesBroker<GobblinScopeTypes> broker) {
      _instanceBroker = Optional.of(broker);
      return this;
    }

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

    public StandardGobblinInstanceLauncher build() {
      return new StandardGobblinInstanceLauncher(getInstanceName(), getSysConfig(), driver(),
          isInstrumentationEnabled() ? Optional.of(getMetricContext()) : Optional.<MetricContext>absent(),
          Optional.of(getLog()), getInstanceBroker());
    }

    @Override
    public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
      return Collections.emptyList();
    }

    @Override
    public void switchMetricContext(List<Tag<?>> tags) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void switchMetricContext(MetricContext context) {
      throw new UnsupportedOperationException();
    }

  }

  @Override public MetricContext getMetricContext() {
    return _metricContext;
  }

  @Override public boolean isInstrumentationEnabled() {
    return _instrumentationEnabled;
  }

  @Override public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
    return Collections.emptyList();
  }

  @Override public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Logger getLog() {
    return _log;
  }

  @Override
  public Configurable getSysConfig() {
    return this;
  }

}
