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

package gobblin.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

import gobblin.configuration.State;


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
    Map<String, Object> typedProps = guessPropertiesTypes(properties);
    ImmutableMap.Builder<String, Object> immutableMapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, Object> entry : typedProps.entrySet()) {
      if (StringUtils.startsWith(entry.getKey(), prefix.or(StringUtils.EMPTY))) {
        immutableMapBuilder.put(entry.getKey(), entry.getValue());
      }
    }
    return ConfigFactory.parseMap(immutableMapBuilder.build());
  }

  /** Attempts to guess type types of a Properties. By default, typesafe will make all property
   * values Strings. This implementation will try to recognize booleans and numbers. All keys are
   * treated as strings.*/
  private static Map<String, Object> guessPropertiesTypes(Map<Object, Object> srcProperties) {
    Map<String, Object> res = new HashMap<>();
    for (Map.Entry<Object, Object> prop: srcProperties.entrySet()) {
      Object value = prop.getValue();
      if (null != value && value instanceof String && !Strings.isNullOrEmpty(value.toString())) {
        try {
          value = Long.parseLong(value.toString());
        }
        catch (NumberFormatException e) {
          try {
            value = Double.parseDouble(value.toString());
          }
          catch (NumberFormatException e2) {
            if (value.toString().equalsIgnoreCase("true") || value.toString().equalsIgnoreCase("yes")) {
              value = Boolean.TRUE;
            }
            else if (value.toString().equalsIgnoreCase("false") || value.toString().equalsIgnoreCase("no")) {
              value = Boolean.FALSE;
            }
            else {
              // nothing to do
            }
          }
        }
      }
      res.put(prop.getKey().toString(), value);
    }
    return res;
  }

  /**
   * Return string value at <code>path</code> if <code>config</code> has path. If not return an empty string
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return string value at <code>path</code> if <code>config</code> has path. If not return an empty string
   */
  public static String emptyIfNotPresent(Config config, String path) {
    return getString(config, path, StringUtils.EMPTY);
  }

  /**
   * Return string value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return string value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   */
  public static String getString(Config config, String path, String def) {
    if (config.hasPath(path)) {
      return config.getString(path);
    }
    return def;
  }

  /**
   * Return {@link Long} value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return {@link Long} value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   */
  public static Long getLong(Config config, String path, Long def) {
    if (config.hasPath(path)) {
      return Long.valueOf(config.getLong(path));
    }
    return def;
  }

  /**
   * Return {@link Integer} value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return {@link Integer} value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   */
  public static Integer getInt(Config config, String path, Integer def) {
    if (config.hasPath(path)) {
      return Integer.valueOf(config.getInt(path));
    }
    return def;
  }

  /**
   * Return boolean value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return boolean value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   */
  public static boolean getBoolean(Config config, String path, boolean def) {
    if (config.hasPath(path)) {
      return config.getBoolean(path);
    }
    return def;
  }

  /**
   * Return {@link Config} value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return config value at <code>path</code> if <code>config</code> has path. If not return <code>def</code>
   */
  public static Config getConfig(Config config, String path, Config def) {
    if (config.hasPath(path)) {
      return config.getConfig(path);
    }
    return def;
  }

  /**
   * An extension to {@link Config#getStringList(String)}. The string list can either be specified as a TypeSafe {@link ConfigList} of strings
   * or as list of comma separated strings.
   *
   * Returns an empty list of <code>path</code> does not exist
   *
   * @param config in which the path may be present
   * @param path key to look for in the config object
   * @return list of strings
   */
  public static List<String> getStringList(Config config, String path) {

    if (!config.hasPath(path)) {
      return Collections.emptyList();
    }

    try {
      return config.getStringList(path);
    } catch (ConfigException.WrongType e) {
      Splitter tokenSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
      return tokenSplitter.splitToList(config.getString(path));
    }

  }
  /**
   * Check if the given <code>key</code> exists in <code>config</code> and it is not null or empty
   * Uses {@link StringUtils#isNotBlank(CharSequence)}
   * @param config which may have the key
   * @param key to look for in the config
   *
   * @return True if key exits and not null or empty. False otherwise
   */
  public static boolean hasNonEmptyPath(Config config, String key) {
    return config.hasPath(key) && StringUtils.isNotBlank(config.getString(key));
  }
}
