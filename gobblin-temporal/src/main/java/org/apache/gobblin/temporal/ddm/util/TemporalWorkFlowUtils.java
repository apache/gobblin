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
package org.apache.gobblin.temporal.ddm.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;


/**
 * Utility class for handling Temporal workflow-related operations.
 */
@UtilityClass
public class TemporalWorkFlowUtils {

  public static final String DEFAULT_GAAS_FLOW_GROUP = "DEFAULT_GAAS_FLOW_GROUP";
  public static final String DEFAULT_GAAS_FLOW_NAME = "DEFAULT_GAAS_FLOW_NAME";
  public static final String DEFAULT_GAAS_USER_TO_PROXY = "DEFAULT_GAAS_USER_TO_PROXY";

  /**
   * Generates search attributes for a WorkFlow  based on the provided GAAS job properties.
   *
   * @param jobProps the properties of the job, must not be null.
   * @return a map containing the generated search attributes.
   */
  public static Map<String, Object> generateGaasSearchAttributes(@NonNull Properties jobProps) {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(Help.GAAS_FLOW_ID_SEARCH_KEY,
        String.format("%s.%s", jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, DEFAULT_GAAS_FLOW_GROUP),
            jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY, DEFAULT_GAAS_FLOW_NAME)));
    attributes.put(Help.USER_TO_PROXY_SEARCH_KEY, jobProps.getProperty(Help.USER_TO_PROXY_KEY, DEFAULT_GAAS_USER_TO_PROXY));
    return attributes;
  }

  /**
   * Converts search attribute values from a map of lists to a map of objects.
   *
   * @param searchAttributes a map where the keys are attribute names and the values are lists of attribute values.
   *                         Can be null.
   * @return a map where the keys are attribute names and the values are the corresponding attribute values.
   *         If the input map is null, an empty map is returned.
   */
  public static Map<String, Object> convertSearchAttributesValuesFromListToObject(
      Map<String, List<?>> searchAttributes) {
    if (searchAttributes == null) {
      return Collections.emptyMap();
    }
    return new HashMap<>(searchAttributes);
  }
}