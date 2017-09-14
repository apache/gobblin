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

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Utility class for collecting metadata specific to a Azkaban runtime environment.
 */
@Slf4j
public class AzkabanTags {

  public static final ImmutableMap<String, String> PROPERTIES_TO_TAGS_MAP = new ImmutableMap.Builder<String, String>()
      .put(ConfigurationKeys.AZKABAN_PROJECT_NAME, "azkabanProjectName")
      .put(ConfigurationKeys.AZKABAN_FLOW_ID, "azkabanFlowId")
      .put(ConfigurationKeys.AZKABAN_JOB_ID, "azkabanJobId")
      .put(ConfigurationKeys.AZKABAN_EXEC_ID, "azkabanExecId")
      .put(ConfigurationKeys.AZKABAN_URL, "azkabanURL")
      .put(ConfigurationKeys.AZKABAN_FLOW_URL, "azkabanFlowURL")
      .put(ConfigurationKeys.AZKABAN_JOB_URL, "azkabanJobURL")
      .put(ConfigurationKeys.AZKABAN_JOB_EXEC_URL, "azkabanJobExecURL")
      .build();

  /**
   * Uses {@link #getAzkabanTags(Configuration)} with default Hadoop {@link Configuration}
   */
  public static Map<String, String> getAzkabanTags() {
    return getAzkabanTags(new Configuration());
  }

  /**
   * Gets all useful Azkaban runtime properties required by metrics as a {@link Map}.
   * Below metrics will be fetched if available:
   * - azkabanFlowId : name of Azkaban flow
   * - azkabanFlowURL : URL of Azkaban flow
   * - azkabanURL : URL of Azkaban flow execution
   * - azkabanExecId : ID of flow execution
   * - azkabanJobId : name of Azkaban job
   * - azkabanJobURL : URL of Azkaban job
   * - azkabanJobExecURL : URL of Azkaban job execution
   *
   * @param conf Hadoop Configuration that contains the properties. Keys of {@link #PROPERTIES_TO_TAGS_MAP} lists out
   * all the properties to look for in {@link Configuration}.
   *
   * @return a {@link Map} with keys as property names (name mapping in {@link #PROPERTIES_TO_TAGS_MAP}) and the value
   * of the property from {@link Configuration}
   */
  public static Map<String, String> getAzkabanTags(Configuration conf) {
    Map<String, String> tagMap = Maps.newHashMap();

    for (Map.Entry<String, String> entry : PROPERTIES_TO_TAGS_MAP.entrySet()) {
      if (StringUtils.isNotBlank(conf.get(entry.getKey()))) {
        tagMap.put(entry.getValue(), conf.get(entry.getKey()));
      } else {
        log.warn(String.format("No config value found for config %s. Metrics will not have tag %s", entry.getKey(),
            entry.getValue()));
      }
    }
    return tagMap;
  }
}
