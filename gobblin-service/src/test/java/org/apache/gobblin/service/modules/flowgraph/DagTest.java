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
import org.testng.collections.Sets;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;


@Slf4j
public class DagTest {
  @Test
  public void testInitialize() {
    DagNode<String> dagNode1 = new DagNode<>("val1");
    DagNode<String> dagNode2 = new DagNode<>("val2");
    DagNode<String> dagNode3 = new DagNode<>("val3");
    DagNode<String> dagNode4 = new DagNode<>("val4");
    DagNode<String> dagNode5 = new DagNode<>("val5");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);
    dagNode4.addParentNode(dagNode2);
    dagNode4.addParentNode(dagNode3);
    dagNode5.addParentNode(dagNode3);

    List<DagNode<String>> dagNodeList = Lists.newArrayList(dagNode1, dagNode2, dagNode3, dagNode4, dagNode5);
    Dag<String> dag = new Dag<>(dagNodeList);
    //Test startNodes and endNodes
    Assert.assertEquals(dag.getStartNodes().size(), 1);
    Assert.assertEquals(dag.getStartNodes().get(0).getValue(), "val1");
    Assert.assertEquals(dag.getEndNodes().size(), 2);
    Assert.assertEquals(dag.getEndNodes().get(0).getValue(), "val4");
    Assert.assertEquals(dag.getEndNodes().get(1).getValue(), "val5");


    DagNode startNode = dag.getStartNodes().get(0);
    Assert.assertEquals(dag.getChildren(startNode).size(), 2);
    Set<String> childSet = new HashSet<>();
    for (DagNode<String> node: dag.getChildren(startNode)) {
      childSet.add(node.getValue());
    }
    Assert.assertTrue(childSet.contains("val2"));
    Assert.assertTrue(childSet.contains("val3"));

    dagNode2 = dag.getChildren(startNode).get(0);
    dagNode3 = dag.getChildren(startNode).get(1);

    Assert.assertEquals(dag.getChildren(dagNode2).size(), 1);
    Assert.assertEquals(dag.getChildren(dagNode2).get(0).getValue(), "val4");

    for (DagNode<String> node: dag.getChildren(dagNode3)) {
      childSet.add(node.getValue());
    }
    Assert.assertTrue(childSet.contains("val4"));
    Assert.assertTrue(childSet.contains("val5"));

    //Ensure end nodes have no children
    Assert.assertEquals(dag.getChildren(dagNode4).size(), 0);
    Assert.assertEquals(dag.getChildren(dagNode5).size(), 0);
  }

  @Test
  public void testConcatenate() {
    DagNode<String> dagNode1 = new DagNode<>("val1");
    DagNode<String> dagNode2 = new DagNode<>("val2");
    DagNode<String> dagNode3 = new DagNode<>("val3");
    DagNode<String> dagNode4 = new DagNode<>("val4");
    DagNode<String> dagNode5 = new DagNode<>("val5");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);
    dagNode4.addParentNode(dagNode2);
    dagNode4.addParentNode(dagNode3);
    dagNode5.addParentNode(dagNode3);

    List<DagNode<String>> dagNodeList = Lists.newArrayList(dagNode1, dagNode2, dagNode3, dagNode4, dagNode5);
    Dag<String> dag1 = new Dag<>(dagNodeList);

    DagNode<String> dagNode6 = new DagNode<>("val6");
    DagNode<String> dagNode7 = new DagNode<>("val7");
    DagNode<String> dagNode8 = new DagNode<>("val8");
    dagNode8.addParentNode(dagNode6);
    dagNode8.addParentNode(dagNode7);
    Dag<String> dag2 = new Dag<>(Lists.newArrayList(dagNode6, dagNode7, dagNode8));

    Dag<String> dagNew = dag1.concatenate(dag2);

    //Ensure end nodes of first dag are no longer end nodes
    for (DagNode<String> dagNode : Lists.newArrayList(dagNode6, dagNode7)) {
      Assert.assertEquals(dagNew.getParents(dagNode).size(), 2);
      Set<String> set = new HashSet<>();
      set.add(dagNew.getParents(dagNode).get(0).getValue());
      set.add(dagNew.getParents(dagNode).get(1).getValue());
      Assert.assertTrue(set.contains("val4"));
      Assert.assertTrue(set.contains("val5"));
    }

    for (DagNode<String> dagNode : Lists.newArrayList(dagNode4, dagNode5)) {
      Assert.assertEquals(dagNew.getChildren(dagNode).size(), 2);
      Set<String> set = new HashSet<>();
      set.add(dagNew.getChildren(dagNode).get(0).getValue());
      set.add(dagNew.getChildren(dagNode).get(1).getValue());
      Assert.assertTrue(set.contains("val6"));
      Assert.assertTrue(set.contains("val7"));
    }

    for (DagNode<String> dagNode : Lists.newArrayList(dagNode6, dagNode7)) {
      List<DagNode<String>> nextNodes = dagNew.getChildren(dagNode);
      Assert.assertEquals(nextNodes.size(), 1);
      Assert.assertEquals(nextNodes.get(0).getValue(), "val8");
    }

    //Test new start and end nodes.
    Assert.assertEquals(dagNew.getStartNodes().size(), 1);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue(), "val1");

    Assert.assertEquals(dagNew.getEndNodes().size(), 1);
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue(), "val8");
  }

  @Test
  public void testConcatenateForkNodes() {
    DagNode<String> dagNode1 = new DagNode<>("val1");
    DagNode<String> dagNode2 = new DagNode<>("val2");
    DagNode<String> dagNode3 = new DagNode<>("val3");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);

    Dag<String> dag1 = new Dag<>(Lists.newArrayList(dagNode1, dagNode2, dagNode3));
    DagNode<String> dagNode4 = new DagNode<>("val4");
    Dag<String> dag2 = new Dag<>(Lists.newArrayList(dagNode4));

    Set<DagNode<String>> forkNodes = Sets.newHashSet();
    forkNodes.add(dagNode3);
    Dag<String> dagNew = dag1.concatenate(dag2, forkNodes);

    Assert.assertEquals(dagNew.getChildren(dagNode2).size(), 1);
    Assert.assertEquals(dagNew.getChildren(dagNode2).get(0), dagNode4);
    Assert.assertEquals(dagNew.getParents(dagNode4).size(), 1);
    Assert.assertEquals(dagNew.getParents(dagNode4).get(0), dagNode2);
    Assert.assertEquals(dagNew.getEndNodes().size(), 2);
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue(), "val4");
    Assert.assertEquals(dagNew.getEndNodes().get(1).getValue(), "val3");
    Assert.assertEquals(dagNew.getChildren(dagNode3).size(), 0);
  }

  @Test
  public void testMerge() {
    DagNode<String> dagNode1 = new DagNode<>("val1");
    DagNode<String> dagNode2 = new DagNode<>("val2");
    DagNode<String> dagNode3 = new DagNode<>("val3");
    DagNode<String> dagNode4 = new DagNode<>("val4");
    DagNode<String> dagNode5 = new DagNode<>("val5");

    dagNode2.addParentNode(dagNode1);
    dagNode3.addParentNode(dagNode1);
    dagNode4.addParentNode(dagNode2);
    dagNode4.addParentNode(dagNode3);
    dagNode5.addParentNode(dagNode3);

    List<DagNode<String>> dagNodeList = Lists.newArrayList(dagNode1, dagNode2, dagNode3, dagNode4, dagNode5);
    Dag<String> dag1 = new Dag<>(dagNodeList);

    DagNode<String> dagNode6 = new DagNode<>("val6");
    DagNode<String> dagNode7 = new DagNode<>("val7");
    DagNode<String> dagNode8 = new DagNode<>("val8");
    dagNode8.addParentNode(dagNode6);
    dagNode8.addParentNode(dagNode7);
    Dag<String> dag2 = new Dag<>(Lists.newArrayList(dagNode6, dagNode7, dagNode8));

    //Merge the two dags
    Dag<String> dagNew = dag1.merge(dag2);

    //Test the startNodes
    Assert.assertEquals(dagNew.getStartNodes().size(), 3);
    for (DagNode<String> dagNode: Lists.newArrayList(dagNode1, dagNode6, dagNode7)) {
      Assert.assertTrue(dagNew.getStartNodes().contains(dagNode));
      Assert.assertEquals(dagNew.getParents(dagNode).size(), 0);
      if (dagNode == dagNode1) {
        List<DagNode<String>> nextNodes = dagNew.getChildren(dagNode);
        Assert.assertEquals(nextNodes.size(), 2);
        Assert.assertTrue(nextNodes.contains(dagNode2));
        Assert.assertTrue(nextNodes.contains(dagNode3));
      } else {
        Assert.assertEquals(dagNew.getChildren(dagNode).size(), 1);
        Assert.assertTrue(dagNew.getChildren(dagNode).contains(dagNode8));
      }
    }

    //Test the endNodes
    Assert.assertEquals(dagNew.getEndNodes().size(), 3);
    for (DagNode<String> dagNode: Lists.newArrayList(dagNode4, dagNode5, dagNode8)) {
      Assert.assertTrue(dagNew.getEndNodes().contains(dagNode));
      Assert.assertEquals(dagNew.getChildren(dagNode).size(), 0);
      if (dagNode == dagNode8) {
        Assert.assertEquals(dagNew.getParents(dagNode).size(), 2);
        Assert.assertTrue(dagNew.getParents(dagNode).contains(dagNode6));
        Assert.assertTrue(dagNew.getParents(dagNode).contains(dagNode7));
      } else {
        Assert.assertTrue(dagNew.getParents(dagNode).contains(dagNode3));
        if (dagNode == dagNode4) {
          Assert.assertEquals(dagNew.getParents(dagNode).size(), 2);
          Assert.assertTrue(dagNew.getParents(dagNode).contains(dagNode2));
        } else {
          Assert.assertEquals(dagNew.getParents(dagNode).size(), 1);
        }
      }
    }

    //Test the other nodes
    Assert.assertEquals(dagNew.getChildren(dagNode2).size(), 1);
    Assert.assertTrue(dagNew.getChildren(dagNode2).contains(dagNode4));
    Assert.assertEquals(dagNew.getChildren(dagNode3).size(), 2);
    Assert.assertTrue(dagNew.getChildren(dagNode3).contains(dagNode4));
    Assert.assertTrue(dagNew.getChildren(dagNode3).contains(dagNode5));
  }
}