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

package org.apache.gobblin.service.modules.restli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;
import com.typesafe.config.Config;

import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;


public class FlowConfigUtils {
  private static final String FLOWCONFIG = "fc";
  private static final String FLOWCONFIG_ID = FLOWCONFIG + '-' + "id";
  private static final String FLOWCONFIG_ID_NAME = FLOWCONFIG_ID + '-' + "name";
  private static final String FLOWCONFIG_ID_GROUP = FLOWCONFIG_ID + '-' + "group";

  private static final String FLOWCONFIG_SCHEDULE = FLOWCONFIG + '-' + "sch";
  private static final String FLOWCONFIG_SCHEDULE_CRON = FLOWCONFIG_SCHEDULE + '-' + "cron";
  private static final String FLOWCONFIG_SCHEDULE_RUN_IMMEDIATELY = FLOWCONFIG_SCHEDULE + '-' + "runImmediately";

  private static final String FLOWCONFIG_TEMPLATEURIS = FLOWCONFIG + '-' + "templateUris";

  public static String serializeFlowId(FlowId id) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(FLOWCONFIG_ID_NAME, id.getFlowName());
    properties.setProperty(FLOWCONFIG_ID_GROUP, id.getFlowGroup());

    return PropertiesUtils.serialize(properties);
  }

  public static FlowId deserializeFlowId(String serialized) throws IOException {
    Properties properties = PropertiesUtils.deserialize(serialized);
    FlowId id =  new FlowId();
    id.setFlowName(properties.getProperty(FLOWCONFIG_ID_NAME));
    id.setFlowGroup(properties.getProperty(FLOWCONFIG_ID_GROUP));
    return id;
  }

  public static String serializeFlowConfig(FlowConfig flowConfig) throws IOException {
    Properties properties = new Properties();
    properties.putAll(flowConfig.getProperties());
    properties.setProperty(FLOWCONFIG_ID_NAME, flowConfig.getId().getFlowName());
    properties.setProperty(FLOWCONFIG_ID_GROUP, flowConfig.getId().getFlowGroup());

    if (flowConfig.hasSchedule()) {
      properties.setProperty(FLOWCONFIG_SCHEDULE_CRON, flowConfig.getSchedule().getCronSchedule());
      properties.setProperty(FLOWCONFIG_SCHEDULE_RUN_IMMEDIATELY, Boolean.toString(flowConfig.getSchedule().isRunImmediately()));
    }

    if (flowConfig.hasTemplateUris()) {
      properties.setProperty(FLOWCONFIG_TEMPLATEURIS, flowConfig.getTemplateUris());
    }

    return PropertiesUtils.serialize(properties);
  }

  public static FlowConfig deserializeFlowConfig(String serialized) throws IOException {
    Properties properties = PropertiesUtils.deserialize(serialized);
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId()
        .setFlowName(properties.getProperty(FLOWCONFIG_ID_NAME))
        .setFlowGroup(properties.getProperty(FLOWCONFIG_ID_GROUP)));

    if (properties.containsKey(FLOWCONFIG_SCHEDULE_CRON)) {
      flowConfig.setSchedule(new Schedule()
          .setCronSchedule(properties.getProperty(FLOWCONFIG_SCHEDULE_CRON))
          .setRunImmediately(Boolean.valueOf(properties.getProperty(FLOWCONFIG_SCHEDULE_RUN_IMMEDIATELY))));
    }

    if (properties.containsKey(FLOWCONFIG_TEMPLATEURIS)) {
      flowConfig.setTemplateUris(properties.getProperty(FLOWCONFIG_TEMPLATEURIS));
    }

    properties.remove(FLOWCONFIG_ID_NAME);
    properties.remove(FLOWCONFIG_ID_GROUP);
    properties.remove(FLOWCONFIG_SCHEDULE_CRON);
    properties.remove(FLOWCONFIG_SCHEDULE_RUN_IMMEDIATELY);
    properties.remove(FLOWCONFIG_TEMPLATEURIS);

    flowConfig.setProperties(new StringMap(Maps.fromProperties(properties)));

    return flowConfig;
  }

  public static List<String> getDataNodes(Config flowConfig, String identifierKey, Map<String, String> dataNodeAliasMap) {
    List<String> dataNodes = ConfigUtils.getStringList(flowConfig, identifierKey);
    return dataNodes.stream().map(dataNode -> dataNodeAliasMap.getOrDefault(dataNode, dataNode)).collect(Collectors.toList());
  }

  public static String getDataNode(Config flowConfig, String identifierKey, Map<String, String> dataNodeAliasMap) {
    String dataNode = ConfigUtils.getString(flowConfig, identifierKey, "");
    return dataNodeAliasMap.getOrDefault(dataNode, dataNode);
  }


}
