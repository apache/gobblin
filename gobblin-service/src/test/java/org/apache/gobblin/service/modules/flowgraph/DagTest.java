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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public class DagTest {
  @Test
  public void testInitialize() throws Exception {
    Dag.DagNode<String> dagNode1 = new Dag.DagNode<>("val1");
    Dag.DagNode<String> dagNode2 = new Dag.DagNode<>("val2");
    Dag.DagNode<String> dagNode3 = new Dag.DagNode<>("val3");
    Dag.DagNode<String> dagNode4 = new Dag.DagNode<>("val4");
    Dag.DagNode<String> dagNode5 = new Dag.DagNode<>("val5");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);
    dagNode4.addParentNode(dagNode2);
    dagNode4.addParentNode(dagNode3);
    dagNode5.addParentNode(dagNode3);

    List<Dag.DagNode<String>> dagNodeList = Lists.newArrayList(dagNode1, dagNode2, dagNode3, dagNode4, dagNode5);
    Dag<String> dag = new Dag<>(dagNodeList);
    //Test startNodes and endNodes
    Assert.assertEquals(dag.getStartNodes().size(),1);
    Assert.assertEquals(dag.getStartNodes().get(0).getValue(), "val1");
    Assert.assertEquals(dag.getEndNodes().size(), 2);
    Assert.assertEquals(dag.getEndNodes().get(0).getValue(), "val4");
    Assert.assertEquals(dag.getEndNodes().get(1).getValue(), "val5");


    Dag.DagNode startNode = dag.getStartNodes().get(0);
    Assert.assertEquals(dag.getChildren(startNode).size(),2);
    Set<String> childSet = new HashSet<>();
    for(Dag.DagNode<String> node: dag.getChildren(startNode)) {
      childSet.add(node.getValue());
    }
    Assert.assertTrue(childSet.contains("val2"));
    Assert.assertTrue(childSet.contains("val3"));

    dagNode2 = dag.getChildren(startNode).get(0);
    dagNode3 = dag.getChildren(startNode).get(1);

    Assert.assertEquals(dag.getChildren(dagNode2).size(), 1);
    Assert.assertEquals(dag.getChildren(dagNode2).get(0).getValue(), "val4");

    for(Dag.DagNode<String> node: dag.getChildren(dagNode3)) {
      childSet.add(node.getValue());
    }
    Assert.assertTrue(childSet.contains("val4"));
    Assert.assertTrue(childSet.contains("val5"));

    //Ensure end nodes have no children
    Assert.assertNull(dag.getChildren(dagNode4));
    Assert.assertNull(dag.getChildren(dagNode5));
  }

  @Test
  public void testConcatenate() throws Exception {
    Dag.DagNode<String> dagNode1 = new Dag.DagNode<>("val1");
    Dag.DagNode<String> dagNode2 = new Dag.DagNode<>("val2");
    Dag.DagNode<String> dagNode3 = new Dag.DagNode<>("val3");
    Dag.DagNode<String> dagNode4 = new Dag.DagNode<>("val4");
    Dag.DagNode<String> dagNode5 = new Dag.DagNode<>("val5");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);
    dagNode4.addParentNode(dagNode2);
    dagNode4.addParentNode(dagNode3);
    dagNode5.addParentNode(dagNode3);

    List<Dag.DagNode<String>> dagNodeList = Lists.newArrayList(dagNode1, dagNode2, dagNode3, dagNode4, dagNode5);
    Dag<String> dag1 = new Dag<>(dagNodeList);

    Dag.DagNode<String> dagNode6 = new Dag.DagNode<>("val6");
    Dag.DagNode<String> dagNode7 = new Dag.DagNode<>("val7");
    Dag.DagNode<String> dagNode8 = new Dag.DagNode<>("val8");
    dagNode8.addParentNode(dagNode6);
    dagNode8.addParentNode(dagNode7);
    Dag<String> dag2 = new Dag<>(Lists.newArrayList(dagNode6, dagNode7, dagNode8));

    //Concatenate the two dags
    Dag<String> dagNew = dag1.concatenate(dag2);

    //Ensure end nodes of first dag are no longer end nodes
    for(Dag.DagNode<String> dagNode: Lists.newArrayList(dagNode6, dagNode7)) {
      Assert.assertEquals(dagNew.getParents(dagNode).size(), 2);
      Set<String> set = new HashSet<>();
      set.add(dagNew.getParents(dagNode).get(0).getValue());
      set.add(dagNew.getParents(dagNode).get(1).getValue());
      Assert.assertTrue(set.contains("val4"));
      Assert.assertTrue(set.contains("val5"));
    }

    for(Dag.DagNode<String> dagNode: Lists.newArrayList(dagNode4, dagNode5)) {
      Assert.assertEquals(dagNew.getChildren(dagNode).size(), 2);
      Set<String> set = new HashSet<>();
      set.add(dagNew.getChildren(dagNode).get(0).getValue());
      set.add(dagNew.getChildren(dagNode).get(1).getValue());
      Assert.assertTrue(set.contains("val6"));
      Assert.assertTrue(set.contains("val7"));
    }

    //Test new start and end nodes.
    Assert.assertEquals(dagNew.getStartNodes().size(),1);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue(), "val1");

    Assert.assertEquals(dagNew.getEndNodes().size(), 1);
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue(), "val8");
  }
}