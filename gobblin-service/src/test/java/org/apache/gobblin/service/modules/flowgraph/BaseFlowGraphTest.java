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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template.StaticFlowTemplate;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.typesafe.config.ConfigFactory;


public class BaseFlowGraphTest {
  private DataNode node1;
  private DataNode node2;
  private DataNode node3;
  private FlowEdge edge1;
  private FlowEdge edge2;
  private FlowEdge edge3;

  BaseFlowGraph graph;
  @BeforeClass
  public void setUp() throws URISyntaxException, ReflectiveOperationException, JobTemplate.TemplateException, SpecNotFoundException,
             IOException {
    Properties properties = new Properties();
    properties.put("key1", "val1");
    node1 = new BaseDataNode("node1", properties);

    properties = new Properties();
    properties.put("key2", "val2");
    node2 = new BaseDataNode("node2", properties);

    properties = new Properties();
    properties.put("key3", "val3");
    node3 = new BaseDataNode("node3", properties);

    FlowTemplate flowTemplate1 = new StaticFlowTemplate(new URI("uri1"),"","", ConfigFactory.empty(),null, null, null);
    FlowTemplate flowTemplate2 = new StaticFlowTemplate(new URI("uri2"),"","", ConfigFactory.empty(),null, null, null);
    FlowTemplate flowTemplate3 = new StaticFlowTemplate(new URI("uri3"),"","", ConfigFactory.empty(),null, null, null);

    //Create edge instances
    edge1 = new BaseFlowEdge(Lists.newArrayList("node1", "node2"), flowTemplate1, null, ConfigFactory.empty(), true);
    edge2 = new BaseFlowEdge(Lists.newArrayList("node2", "node3"), flowTemplate2, null, ConfigFactory.empty(), true);
    edge3 = new BaseFlowEdge(Lists.newArrayList("node3", "node1"), flowTemplate3, null, ConfigFactory.empty(), true);

    //Create a FlowGraph
    graph = new BaseFlowGraph();

    //Add nodes
    graph.addDataNode(node1);
    graph.addDataNode(node2);
    graph.addDataNode(node3);

    //Add edges
    graph.addFlowEdge(edge1);
    graph.addFlowEdge(edge2);
    graph.addFlowEdge(edge3);
  }

  @Test
  public void testAddDataNode() throws Exception {
    Assert.assertTrue(graph.getNodes().contains(node1));
    Assert.assertTrue(graph.getNodes().contains(node2));
    Assert.assertTrue(graph.getNodes().contains(node3));

    Field field = BaseFlowGraph.class.getDeclaredField("dataNodeMap");
    field.setAccessible(true);
    Map<String, DataNode> dataNodeMap = (Map<String, DataNode>) field.get(graph);
    Assert.assertEquals(dataNodeMap.get("node1"), node1);
    Assert.assertEquals(dataNodeMap.get("node2"), node2);
    Assert.assertEquals(dataNodeMap.get("node3"), node3);
  }

  @Test
  public void testAddFlowEdge() throws Exception {
    Assert.assertEquals(((BaseDataNode)graph.getNode("node1")).getFlowEdges().size(), 1);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node2")).getFlowEdges().size(), 1);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node3")).getFlowEdges().size(), 1);

    Assert.assertTrue(((BaseDataNode)graph.getNode("node1")).getFlowEdges().contains(edge1));
    Assert.assertTrue(((BaseDataNode)graph.getNode("node2")).getFlowEdges().contains(edge2));
    Assert.assertTrue(((BaseDataNode)graph.getNode("node3")).getFlowEdges().contains(edge3));
  }

  @Test
  public void testDeleteDataNode() throws Exception {
    //Delete node1 from graph
    graph.deleteDataNode(node1);
    Assert.assertTrue(!graph.getNodes().contains(node1));
    Assert.assertTrue(graph.getNodes().contains(node2));
    Assert.assertTrue(graph.getNodes().contains(node3));

    Field field = BaseFlowGraph.class.getDeclaredField("dataNodeMap");
    field.setAccessible(true);
    Map<String, DataNode> dataNodeMap = (Map<String, DataNode>) field.get(graph);
    Assert.assertTrue(!dataNodeMap.containsKey("node1"));
    Assert.assertEquals(dataNodeMap.get("node2"), node2);
    Assert.assertEquals(dataNodeMap.get("node3"), node3);

    //Add node back to graph
    graph.addDataNode(node1);
  }

  @Test
  public void testDeleteFlowEdge() throws Exception {
    graph.deleteFlowEdge(edge1);

    Assert.assertEquals(((BaseDataNode)graph.getNode("node1")).getFlowEdges().size(), 0);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node2")).getFlowEdges().size(), 1);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node3")).getFlowEdges().size(), 1);

    Assert.assertTrue(!((BaseDataNode)graph.getNode("node1")).getFlowEdges().contains(edge1));
    Assert.assertTrue(((BaseDataNode)graph.getNode("node2")).getFlowEdges().contains(edge2));
    Assert.assertTrue(((BaseDataNode)graph.getNode("node3")).getFlowEdges().contains(edge3));
    Assert.assertEquals(graph.getNodes().size(), 3);

    graph.deleteFlowEdge(edge2);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node1")).getFlowEdges().size(), 0);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node2")).getFlowEdges().size(), 0);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node3")).getFlowEdges().size(), 1);

    Assert.assertTrue(!((BaseDataNode)graph.getNode("node1")).getFlowEdges().contains(edge1));
    Assert.assertTrue(!((BaseDataNode)graph.getNode("node2")).getFlowEdges().contains(edge2));
    Assert.assertTrue(((BaseDataNode)graph.getNode("node3")).getFlowEdges().contains(edge3));
    Assert.assertEquals(graph.getNodes().size(), 3);

    graph.deleteFlowEdge(edge3);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node1")).getFlowEdges().size(), 0);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node2")).getFlowEdges().size(), 0);
    Assert.assertEquals(((BaseDataNode)graph.getNode("node3")).getFlowEdges().size(), 0);

    Assert.assertTrue(!((BaseDataNode)graph.getNode("node1")).getFlowEdges().contains(edge1));
    Assert.assertTrue(!((BaseDataNode)graph.getNode("node2")).getFlowEdges().contains(edge2));
    Assert.assertTrue(!((BaseDataNode)graph.getNode("node3")).getFlowEdges().contains(edge3));
    Assert.assertEquals(graph.getNodes().size(), 3);
  }
}