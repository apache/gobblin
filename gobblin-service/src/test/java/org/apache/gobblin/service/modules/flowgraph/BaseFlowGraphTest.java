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

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.service.modules.template.StaticFlowTemplate;
import org.apache.gobblin.util.ConfigUtils;


public class BaseFlowGraphTest {
  private DataNode node1;
  private DataNode node2;
  private DataNode node3;
  private DataNode node3c;

  private FlowEdge edge1;
  private FlowEdge edge2;
  private FlowEdge edge3;
  private FlowEdge edge3c;

  BaseFlowGraph graph;
  @BeforeClass
  public void setUp() throws URISyntaxException, ReflectiveOperationException, JobTemplate.TemplateException, SpecNotFoundException,
             IOException {
    Properties properties = new Properties();
    properties.put("key1", "val1");
    node1 = new BaseDataNode("node1", ConfigUtils.propertiesToConfig(properties));

    properties = new Properties();
    properties.put("key2", "val2");
    node2 = new BaseDataNode("node2", ConfigUtils.propertiesToConfig(properties));

    properties = new Properties();
    properties.put("key3", "val3");
    node3 = new BaseDataNode("node3", ConfigUtils.propertiesToConfig(properties));

    //Create a clone of node3
    node3c = new BaseDataNode("node3", ConfigUtils.propertiesToConfig(properties));

    FlowTemplate flowTemplate1 = new StaticFlowTemplate(new URI("FS:///uri1"),"","", ConfigFactory.empty(),null, null, null);
    FlowTemplate flowTemplate2 = new StaticFlowTemplate(new URI("FS:///uri2"),"","", ConfigFactory.empty(),null, null, null);
    FlowTemplate flowTemplate3 = new StaticFlowTemplate(new URI("FS:///uri3"),"","", ConfigFactory.empty(),null, null, null);

    //Create edge instances
    edge1 = new BaseFlowEdge(Lists.newArrayList("node1", "node2"), "edge1", flowTemplate1, null, ConfigFactory.empty(), true);
    edge2 = new BaseFlowEdge(Lists.newArrayList("node2", "node3"), "edge2", flowTemplate2, null, ConfigFactory.empty(), true);
    edge3 = new BaseFlowEdge(Lists.newArrayList("node3", "node1"), "edge3", flowTemplate3, null, ConfigFactory.empty(), true);

    //Create a clone of edge3
    edge3c = new BaseFlowEdge(Lists.newArrayList("node3", "node1"), "edge3", flowTemplate3, null, ConfigFactory.empty(), true);

    //Create a FlowGraph
    graph = new BaseFlowGraph();

    //Add nodes
    Assert.assertTrue(graph.addDataNode(node1));
    Assert.assertTrue(graph.addDataNode(node2));
    Assert.assertTrue(graph.addDataNode(node3));

    Assert.assertEquals(graph.getEdges(node1).size(), 0);
    Assert.assertEquals(graph.getEdges(node2).size(), 0);
    Assert.assertEquals(graph.getEdges(node3).size(), 0);

    //Add edges
    Assert.assertTrue(graph.addFlowEdge(edge1));
    Assert.assertTrue(graph.addFlowEdge(edge2));
    Assert.assertTrue(graph.addFlowEdge(edge3));
  }

  @Test
  public void testAddDataNode() throws Exception {
    //Check nodes
    Assert.assertTrue(graph.getNodes().contains(node1));
    Assert.assertTrue(graph.getNodes().contains(node2));
    Assert.assertTrue(graph.getNodes().contains(node3));

    //Check contents of dataNodeMap
    Field field = BaseFlowGraph.class.getDeclaredField("dataNodeMap");
    field.setAccessible(true);
    Map<String, DataNode> dataNodeMap = (Map<String, DataNode>) field.get(graph);
    Assert.assertEquals(dataNodeMap.get("node1"), node1);
    Assert.assertEquals(dataNodeMap.get("node2"), node2);
    Assert.assertEquals(dataNodeMap.get("node3"), node3);

    graph.addDataNode(node3c);
    Assert.assertEquals(graph.getNode("node3"), node3);
    Assert.assertEquals(graph.getNode("node3"), node3c);

    //Ensure the cloned node overwrites the original
    Assert.assertTrue(graph.getNode("node3") == node3c);
    Assert.assertTrue(graph.getNode("node3") != node3);

    //Add back original node
    graph.addDataNode(node3);
  }

  @Test (dependsOnMethods = "testAddDataNode")
  public void testAddFlowEdge() throws Exception {
    //Check nodesToEdges
    Assert.assertEquals(graph.getEdges("node1").size(), 1);
    Assert.assertEquals(graph.getEdges("node2").size(), 1);
    Assert.assertEquals(graph.getEdges("node3").size(), 1);

    Assert.assertTrue(graph.getEdges("node1").contains(edge1));
    Assert.assertTrue(graph.getEdges("node2").contains(edge2));
    Assert.assertTrue(graph.getEdges("node3").contains(edge3));

    //Try adding an edge that already exists
    Assert.assertTrue(graph.addFlowEdge(edge3c));
    Assert.assertTrue(graph.getEdges("node3").contains(edge3));
    //graph should contain the new copy of the edge
    Assert.assertTrue(graph.getEdges("node3").iterator().next() == edge3c);
    Assert.assertTrue(edge3 != edge3c);

    //Add back original edge
    Assert.assertTrue(graph.addFlowEdge(edge3));

    //Check contents of flowEdgeMap
    Field field = BaseFlowGraph.class.getDeclaredField("flowEdgeMap");
    field.setAccessible(true);
    Map<String, FlowEdge> flowEdgeMap = (Map<String, FlowEdge>) field.get(graph);
    Assert.assertEquals(flowEdgeMap.get(edge1.getId()), edge1);
    Assert.assertEquals(flowEdgeMap.get(edge2.getId()), edge2);
    Assert.assertEquals(flowEdgeMap.get(edge3.getId()), edge3);
  }

  @Test (dependsOnMethods = "testAddFlowEdge")
  public void testDeleteDataNode() throws Exception {
    //Delete node1 from graph
    Assert.assertTrue(graph.deleteDataNode(node1));

    //Check contents of nodes
    Assert.assertTrue(!graph.getNodes().contains(node1));
    Assert.assertTrue(graph.getNodes().contains(node2));
    Assert.assertTrue(graph.getNodes().contains(node3));

    //Check contents of dataNodeMap
    Assert.assertEquals(graph.getNode(node1.getId()), null);
    Assert.assertEquals(graph.getNode(node2.getId()), node2);
    Assert.assertEquals(graph.getNode(node3.getId()), node3);

    //Check contents of nodesToEdges
    Assert.assertEquals(graph.getEdges(node1), null);

    //Check contents of dataNodeMap
    Field field = BaseFlowGraph.class.getDeclaredField("dataNodeMap");
    field.setAccessible(true);
    Map<String, DataNode> dataNodeMap = (Map<String, DataNode>) field.get(graph);
    Assert.assertTrue(!dataNodeMap.containsKey("node1"));
    Assert.assertEquals(dataNodeMap.get("node2"), node2);
    Assert.assertEquals(dataNodeMap.get("node3"), node3);

    //Check contents of flowEdgeMap. Ensure edge1 is no longer in flowEdgeMap
    Assert.assertTrue(!graph.deleteFlowEdge(edge1));
    field = BaseFlowGraph.class.getDeclaredField("flowEdgeMap");
    field.setAccessible(true);
    Map<String, FlowEdge> flowEdgeMap = (Map<String, FlowEdge>) field.get(graph);
    Assert.assertTrue(!flowEdgeMap.containsKey(edge1.getId()));
    Assert.assertEquals(flowEdgeMap.get(edge2.getId()), edge2);
    Assert.assertEquals(flowEdgeMap.get(edge3.getId()), edge3);

    //Add node1 and edge1 back to the graph
    graph.addDataNode(node1);
    graph.addFlowEdge(edge1);
  }

  @Test (dependsOnMethods = "testDeleteDataNode")
  public void testDeleteFlowEdge() throws Exception {
    //Delete edge 1
    Assert.assertTrue(graph.deleteFlowEdge(edge1));
    Assert.assertEquals(graph.getEdges(node1).size(), 0);
    Assert.assertEquals(graph.getEdges(node2).size(), 1);
    Assert.assertEquals(graph.getEdges(node3).size(), 1);

    Assert.assertTrue(!graph.getEdges(node1).contains(edge1));
    Assert.assertTrue(graph.getEdges(node2).contains(edge2));
    Assert.assertTrue(graph.getEdges(node3).contains(edge3));

    //Delete edge2
    Assert.assertTrue(graph.deleteFlowEdge(edge2));
    Assert.assertEquals(graph.getEdges(node1).size(), 0);
    Assert.assertEquals(graph.getEdges(node2).size(), 0);
    Assert.assertEquals(graph.getEdges(node3).size(), 1);

    Assert.assertTrue(!graph.getEdges(node1).contains(edge1));
    Assert.assertTrue(!graph.getEdges(node2).contains(edge2));
    Assert.assertTrue(graph.getEdges(node3).contains(edge3));

    //Delete edge3
    Assert.assertTrue(graph.deleteFlowEdge(edge3));
    Assert.assertEquals(graph.getEdges(node1).size(), 0);
    Assert.assertEquals(graph.getEdges(node2).size(), 0);
    Assert.assertEquals(graph.getEdges(node3).size(), 0);

    Assert.assertTrue(!graph.getEdges(node1).contains(edge1));
    Assert.assertTrue(!graph.getEdges(node2).contains(edge2));
    Assert.assertTrue(!graph.getEdges(node3).contains(edge3));

    //Ensure we cannot delete already deleted edges
    Assert.assertTrue(!graph.deleteFlowEdge(edge1));
    Assert.assertTrue(!graph.deleteFlowEdge(edge2));
    Assert.assertTrue(!graph.deleteFlowEdge(edge3));

    //Add back the edges
    graph.addFlowEdge(edge1);
    graph.addFlowEdge(edge2);
    graph.addFlowEdge(edge3);
  }

  @Test (dependsOnMethods = "testDeleteFlowEdge")
  public void testDeleteFlowEdgeById() throws Exception {
    String edgeLabel1 = BaseFlowEdge.generateEdgeId(Lists.newArrayList("node1", "node2"), "edge1");
    Assert.assertTrue(graph.deleteFlowEdge(edgeLabel1));
    Assert.assertEquals(graph.getEdges("node1").size(), 0);
    Assert.assertEquals(graph.getEdges("node2").size(), 1);
    Assert.assertEquals(graph.getEdges("node3").size(), 1);

    Assert.assertTrue(!graph.getEdges("node1").contains(edge1));
    Assert.assertTrue(graph.getEdges("node2").contains(edge2));
    Assert.assertTrue(graph.getEdges("node3").contains(edge3));

    String edgeLabel2 = BaseFlowEdge.generateEdgeId(Lists.newArrayList("node2", "node3"), "edge2");
    Assert.assertTrue(graph.deleteFlowEdge(edgeLabel2));
    Assert.assertEquals(graph.getEdges("node1").size(), 0);
    Assert.assertEquals(graph.getEdges("node2").size(), 0);
    Assert.assertEquals(graph.getEdges("node3").size(), 1);

    Assert.assertTrue(!graph.getEdges("node1").contains(edge1));
    Assert.assertTrue(!graph.getEdges("node2").contains(edge2));
    Assert.assertTrue(graph.getEdges("node3").contains(edge3));

    String edgeLabel3 = BaseFlowEdge.generateEdgeId(Lists.newArrayList("node3", "node1"), "edge3");
    Assert.assertTrue(graph.deleteFlowEdge(edgeLabel3));
    Assert.assertEquals(graph.getEdges("node1").size(), 0);
    Assert.assertEquals(graph.getEdges("node2").size(), 0);
    Assert.assertEquals(graph.getEdges("node3").size(), 0);

    Assert.assertTrue(!graph.getEdges("node1").contains(edge1));
    Assert.assertTrue(!graph.getEdges("node2").contains(edge2));
    Assert.assertTrue(!graph.getEdges("node3").contains(edge3));

    Assert.assertTrue(!graph.deleteFlowEdge(edgeLabel1));
    Assert.assertTrue(!graph.deleteFlowEdge(edgeLabel2));
    Assert.assertTrue(!graph.deleteFlowEdge(edgeLabel3));
  }
}