/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.instance;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.GobblinInstanceLauncher;
import gobblin.runtime.std.DefaultConfigurableImpl;

/**
 * A standard implementation that expects the instance configuration to be passed from the outside.
 * As a driver, it uses {@link StandardGobblinInstanceDriver}.
 */
public class StandardGobblinInstanceLauncher extends AbstractIdleService
      implements GobblinInstanceLauncher {
  private final String _name;
  private final Configurable _instanceConf;
  private final StandardGobblinInstanceDriver _driver;

  protected StandardGobblinInstanceLauncher(String name, Configurable instanceConf,
      StandardGobblinInstanceDriver.Builder driverBuilder) {
    _name = name;
    _instanceConf = instanceConf;
    _driver = driverBuilder.withInstanceLauncher(this).build();
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

  public static class Builder {
    final static AtomicInteger INSTANCE_COUNT = new AtomicInteger(1);

    Optional<String> _name = Optional.absent();
    Optional<Logger> _log = Optional.absent();
    StandardGobblinInstanceDriver.Builder _driver = new StandardGobblinInstanceDriver.Builder();
    Optional<? extends Configurable> _instanceConfig = Optional.absent();

    public String getDefaultInstanceName() {
      return StandardGobblinInstanceLauncher.class.getSimpleName() + "-" +
             INSTANCE_COUNT.getAndIncrement();
    }

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
    public Configurable getDefaultInstanceConfig() {
      return DefaultConfigurableImpl.createFromConfig(ConfigFactory.load());
    }

    public Configurable getInstanceConfig() {
      if (! _instanceConfig.isPresent()) {
        _instanceConfig = Optional.of(getDefaultInstanceConfig());
      }
      return _instanceConfig.get();
    }

    public Builder withInstanceConfig(Config instanceConfig) {
      _instanceConfig = Optional.of(DefaultConfigurableImpl.createFromConfig(instanceConfig));
      return this;
    }

    public Builder withInstanceConfig(Properties instanceConfig) {
      _instanceConfig = Optional.of(DefaultConfigurableImpl.createFromProperties(instanceConfig));
      return this;
    }

    public StandardGobblinInstanceLauncher build() {
      return new StandardGobblinInstanceLauncher(getInstanceName(), getInstanceConfig(), driver());
    }

  }

}
