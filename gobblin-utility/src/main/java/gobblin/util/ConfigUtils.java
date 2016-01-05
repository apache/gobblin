/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import gobblin.configuration.State;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;


/**
 * Utility class for dealing with {@link Config} objects.
 */
public class ConfigUtils {

  /**
   * Convert a given {@link Config} instance to a {@link Properties} instance.
   *
   * @param config the given {@link Config} instance
   * @return a {@link Properties} instance
   */
  public static Properties configToProperties(Config config) {
    Properties properties = new Properties();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), config.getString(entry.getKey()));
    }

    return properties;
  }

  /**
   * Convert a given {@link Config} to a {@link State} instance.
   *
   * @param config the given {@link Config} instance
   * @return a {@link State} instance
   */
  public static State configToState(Config config) {
    return new State(configToProperties(config));
  }

  /**
   * Convert a given {@link Properties} to a {@link Config} instance.
   *
   * <p>
   *   This method will throw an exception if (1) the {@link Object#toString()} method of any two keys in the
   *   {@link Properties} objects returns the same {@link String}, or (2) if any two keys are prefixes of one another,
   *   see the Java Docs of {@link ConfigFactory#parseMap(Map)} for more details.
   * </p>
   *
   * @param properties the given {@link Properties} instance
   * @return a {@link Config} instance
   */
  public static Config propertiesToConfig(Properties properties) {
    return propertiesToConfig(properties, Optional.<String>absent());
  }

  /**
   * Convert all the keys that start with a <code>prefix</code> in {@link Properties} to a {@link Config} instance.
   *
   * <p>
   *   This method will throw an exception if (1) the {@link Object#toString()} method of any two keys in the
   *   {@link Properties} objects returns the same {@link String}, or (2) if any two keys are prefixes of one another,
   *   see the Java Docs of {@link ConfigFactory#parseMap(Map)} for more details.
   * </p>
   *
   * @param properties the given {@link Properties} instance
   * @param prefix of keys to be converted
   * @return a {@link Config} instance
   */
  public static Config propertiesToConfig(Properties properties, Optional<String> prefix) {
    ImmutableMap.Builder<String, Object> immutableMapBuilder = ImmutableMap.builder();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      if (StringUtils.startsWith(entry.getKey().toString(), prefix.or(StringUtils.EMPTY))) {
        immutableMapBuilder.put(entry.getKey().toString(), entry.getValue());
      }
    }
    return ConfigFactory.parseMap(immutableMapBuilder.build());
  }
}
