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
package org.apache.gobblin.runtime.api;

import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.annotation.Alpha;

import lombok.Getter;

/**
 * Main class instantiated by the JVM or running framework. Reads application level
 *  configurations, instantiates and runs the Gobblin instance driver.
 */
@Alpha
public interface GobblinInstanceLauncher extends Service, Configurable, GobblinInstanceEnvironment {
  /**
   * Creates a new Gobblin instance to run Gobblin jobs.
   * @throws IllegalStateException if {@link #isRunning()} is false.*/
  GobblinInstanceDriver getDriver() throws IllegalStateException;

  // Standard configuration for Gobblin instances
  @Getter
  public static class ConfigAccessor {
    static final String RESOURCE_NAME =
        GobblinInstanceLauncher.class.getPackage().getName().replaceAll("[.]", "/")
        + "/"
        + GobblinInstanceLauncher.class.getSimpleName()
        + ".conf";
    /** The namespace for all config options */
    static final String CONFIG_PREFIX = "gobblin.instance";
    /** The time to wait for gobblin instance components to start in milliseconds. */
    static final String START_TIMEOUT_MS = "startTimeoutMs";
    /** The time wait for Gobblin components to shutdown in milliseconds. */
    static final String SHUTDOWN_TIMEOUT_MS = "shutdownTimeoutMs";

    private final long startTimeoutMs;
    private final long shutdownTimeoutMs;

    /** Config accessor from a no namespaced typesafe config. */
    public ConfigAccessor(Config cfg) {
      Config effectiveCfg = cfg.withFallback(getDefaultConfig().getConfig(CONFIG_PREFIX));
      this.startTimeoutMs = effectiveCfg.getLong(START_TIMEOUT_MS);
      this.shutdownTimeoutMs = effectiveCfg.getLong(SHUTDOWN_TIMEOUT_MS);
    }

    public static Config getDefaultConfig() {
      return ConfigFactory.parseResources(GobblinInstanceLauncher.class,
          GobblinInstanceLauncher.class.getSimpleName() + ".conf").withFallback(ConfigFactory.load());
    }

    public static ConfigAccessor createFromGlobalConfig(Config cfg) {
      Config localCfg = cfg.hasPath(CONFIG_PREFIX) ? cfg.getConfig(CONFIG_PREFIX) :
        ConfigFactory.empty();
      return new ConfigAccessor(localCfg);
    }
  }
}
