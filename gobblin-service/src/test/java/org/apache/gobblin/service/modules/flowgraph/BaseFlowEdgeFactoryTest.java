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

package org.apache.gobblin.service.modules.flowgraph;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.util.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseFlowEdgeFactoryTest {
  @Test
  public void testCreateFlowEdge() throws Exception {
    Properties properties = new Properties();
    properties.put(FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY,"node1");
    properties.put(FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY, "node2");
    properties.put(FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY, "edge1");
    properties.put(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "node1:node2:edge1");
    properties.put(FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_DIR_URI_KEY, "FS:///flowEdgeTemplate");

    List<SpecExecutor> specExecutorList = new ArrayList<>();
    Config config1 = ConfigFactory.empty().withValue("specStore.fs.dir", ConfigValueFactory.fromAnyRef("/tmp1")).
        withValue("specExecInstance.capabilities", ConfigValueFactory.fromAnyRef("s1:d1"));
    specExecutorList.add(new InMemorySpecExecutor(config1));
    Config config2 = ConfigFactory.empty().withValue("specStore.fs.dir", ConfigValueFactory.fromAnyRef("/tmp2")).
        withValue("specExecInstance.capabilities", ConfigValueFactory.fromAnyRef("s2:d2"));
    specExecutorList.add(new InMemorySpecExecutor(config2));

    FlowEdgeFactory flowEdgeFactory = new BaseFlowEdge.Factory();

    Properties props = new Properties();
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    props.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(props);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowTemplateCatalog catalog = new FSFlowTemplateCatalog(templateCatalogCfg);
    Config edgeProps = ConfigUtils.propertiesToConfig(properties);
    FlowEdge flowEdge = flowEdgeFactory.createFlowEdge(edgeProps, catalog, specExecutorList);
    Assert.assertEquals(flowEdge.getSrc(), "node1");
    Assert.assertEquals(flowEdge.getDest(), "node2");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specStore.fs.dir"),"/tmp1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specExecInstance.capabilities"),"s1:d1");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specStore.fs.dir"),"/tmp2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specExecInstance.capabilities"),"s2:d2");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getClass().getSimpleName(),"InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getClass().getSimpleName(),"InMemorySpecExecutor");
  }
}