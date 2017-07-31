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
package org.apache.gobblin.util.service;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A wrapper around a typesafe {@link Config} object to provide standard configuration for
 * services.
 */
@Getter @EqualsAndHashCode
public class StandardServiceConfig {
  public static final String STARTUP_PREFIX = "startUp";
  public static final String TIMEOUT_MS_KEY = "timeoutMs";
  public static final String STARTUP_TIMEOUT_MS_PROP = STARTUP_PREFIX + "." + TIMEOUT_MS_KEY;
  public static final String SHUTDOWN_PREFIX = "shutDown";
  public static final String SHUTDOWN_TIMEOUT_MS_PROP = SHUTDOWN_PREFIX + "." + TIMEOUT_MS_KEY;
  public static final Config DEFAULT_CFG =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(STARTUP_TIMEOUT_MS_PROP,  5 * 60 * 1000)  // 5 minutes
          .put(SHUTDOWN_TIMEOUT_MS_PROP,  5 * 60 * 1000) // 5 minutes
          .build());

  private final long startUpTimeoutMs;
  private final long shutDownTimeoutMs;

  /**
   * Constructor from a typesafe config object
   * @param serviceCfg    the service configuration; must be local, i.e. any service namespace
   *                      prefix should be removed using {@link Config#getConfig(String)}.
   **/
  public StandardServiceConfig(Config serviceCfg) {
    Config effectiveCfg = serviceCfg.withFallback(DEFAULT_CFG);
    this.startUpTimeoutMs = effectiveCfg.getLong(STARTUP_TIMEOUT_MS_PROP);
    this.shutDownTimeoutMs = effectiveCfg.getLong(SHUTDOWN_TIMEOUT_MS_PROP);
  }
}
