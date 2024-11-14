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

import java.util.Map;
import java.util.Properties;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableBiMap;

/**
 * Utility class for Azkaban App Launcher.
 */
public class AzkabanLauncherUtils {
  public static final String PLACEHOLDER_MAP_KEY = "placeholderMap";

  public static Properties undoPlaceholderConversion(Properties appProperties) {
    Properties convertedProperties = new Properties();
    convertedProperties.putAll(appProperties);

    // Undo properties converted to placeholders
    Map<String, String> inversePlaceholderMap = ImmutableBiMap.copyOf(Splitter.on(",").withKeyValueSeparator(":")
        .split(convertedProperties.get(PLACEHOLDER_MAP_KEY).toString())).inverse();
    for (Map.Entry<Object, Object> entry : convertedProperties.entrySet()) {
      if (inversePlaceholderMap.containsKey(entry.getValue().toString())) {
        convertedProperties.put(entry.getKey(), inversePlaceholderMap.get(entry.getValue().toString()));
      }
    }
    return convertedProperties;
  }
}
