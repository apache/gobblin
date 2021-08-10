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

package org.apache.gobblin.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;


/**
 * A utility class for {@link Properties} objects.
 */
public class PropertiesUtils {

  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  /**
   * Combine a variable number of {@link Properties} into a single {@link Properties}.
   */
  public static Properties combineProperties(Properties... properties) {
    Properties combinedProperties = new Properties();
    for (Properties props : properties) {
      combinedProperties.putAll(props);
    }
    return combinedProperties;
  }

  /**
   * Converts a {@link Properties} object to a {@link Map} where each key is a {@link String}.
   */
  public static Map<String, ?> propsToStringKeyMap(Properties properties) {
    ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      mapBuilder.put(entry.getKey().toString(), entry.getValue());
    }
    return mapBuilder.build();
  }

  public static boolean getPropAsBoolean(Properties properties, String key, String defaultValue) {
    return Boolean.valueOf(properties.getProperty(key, defaultValue));
  }

  public static int getPropAsInt(Properties properties, String key, int defaultValue) {
    return Integer.parseInt(properties.getProperty(key, Integer.toString(defaultValue)));
  }

  public static long getPropAsLong(Properties properties, String key, long defaultValue) {
    return Long.parseLong(properties.getProperty(key, Long.toString(defaultValue)));
  }

  /**
   * Get the value of a comma separated property as a {@link List} of strings.
   *
   * @param key property key
   * @return value associated with the key as a {@link List} of strings
   */
  public static List<String> getPropAsList(Properties properties, String key) {
    return LIST_SPLITTER.splitToList(properties.getProperty(key));
  }

  /**
   * Extract all the values whose keys start with a <code>prefix</code>
   * @param properties the given {@link Properties} instance
   * @param prefix of keys to be extracted
   * @return a list of values in the properties
   */
  public static List<String> getValuesAsList(Properties properties, Optional<String> prefix) {
    if (prefix.isPresent()) {
      properties = extractPropertiesWithPrefix(properties, prefix);
    }
    Properties finalProperties = properties;
    return properties.keySet().stream().map(key -> finalProperties.getProperty(key.toString())).collect(Collectors.toList());
  }

  /**
   * Get the value of a property as a list of strings, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value (the default value if the property is not set) associated with the key as a list of strings
   */
  public static List<String> getPropAsList(Properties properties, String key, String def) {
    return LIST_SPLITTER.splitToList(properties.getProperty(key, def));
  }

  /**
   * Extract all the keys that start with a <code>prefix</code> in {@link Properties} to a new {@link Properties}
   * instance.
   *
   * @param properties the given {@link Properties} instance
   * @param prefix of keys to be extracted
   * @return a {@link Properties} instance
   */
  public static Properties extractPropertiesWithPrefix(Properties properties, Optional<String> prefix) {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(prefix);

    Properties extractedProperties = new Properties();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      if (StringUtils.startsWith(entry.getKey().toString(), prefix.or(StringUtils.EMPTY))) {
        extractedProperties.put(entry.getKey().toString(), entry.getValue());
      }
    }

    return extractedProperties;
  }

  /**
   * Extract all the keys that start with a <code>prefix</code> in {@link Properties} to a new {@link Properties}
   * instance. It removes the prefix from the properties.
   *
   * @param properties the given {@link Properties} instance
   * @param prefix of keys to be extracted
   * @return a {@link Properties} instance
   */
  public static Properties extractPropertiesWithPrefixAfterRemovingPrefix(Properties properties, String prefix) {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(prefix);

    Properties extractedProperties = new Properties();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      if (StringUtils.startsWith(entry.getKey().toString(), prefix)) {
        extractedProperties.put(entry.getKey().toString().substring(prefix.length()), entry.getValue());
      }
    }

    return extractedProperties;
  }

  public static String serialize(Properties properties) throws IOException {
    StringWriter outputWriter = new StringWriter();
    properties.store(outputWriter, "");
    String rst = outputWriter.toString();
    outputWriter.close();
    return rst;
  }

  public static Properties deserialize(String serialized) throws IOException {
    StringReader reader = new StringReader(serialized);
    Properties properties = new Properties();
    properties.load(reader);
    reader.close();
    return properties;
  }

  public static String prettyPrintProperties(Properties properties) {
    return properties.entrySet().stream()
        .map(entry -> "\"" + entry.getKey() + "\"" + " : " + "\"" + entry.getValue() + "\"")
        .collect(Collectors.joining(",\n"));
  }
}
