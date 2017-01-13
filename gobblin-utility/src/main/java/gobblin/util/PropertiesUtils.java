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

package gobblin.util;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;


/**
 * A utility class for {@link Properties} objects.
 */
public class PropertiesUtils {

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
}
