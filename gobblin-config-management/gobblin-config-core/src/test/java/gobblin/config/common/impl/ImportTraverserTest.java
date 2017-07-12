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

package gobblin.config.common.impl;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;


public class ImportTraverserTest {

  /**
   * Test a simple non-tree graph. Check that all expected nodes are included in a traversal and in the correct order.
   */
  @Test
  public void testSimpleGraph() throws Exception {

    //     a --> b --> d
    //       \-> c -/

    ListMultimap<String, String> edges = LinkedListMultimap.create();
    edges.put("a", "b");
    edges.put("a", "c");
    edges.put("b", "d");
    edges.put("c", "d");

    ImportTraverser<String> traverser = new ImportTraverser<String>(s -> edges.get(s), CacheBuilder.newBuilder().build());

    List<String> traversal = traverser.traverseGraphRecursively("a");
    Assert.assertEquals(traversal, Lists.newArrayList("b", "d", "c"));

    traversal = traverser.traverseGraphRecursively("b");
    Assert.assertEquals(traversal, Lists.newArrayList("d"));

    traversal = traverser.traverseGraphRecursively("c");
    Assert.assertEquals(traversal, Lists.newArrayList("d"));

    traversal = traverser.traverseGraphRecursively("d");
    Assert.assertEquals(traversal, Lists.newArrayList());

  }

  /**
   * Test a graph with cycles. Check attempting to traverse starting at node in the cycle throws exception, while attempting
   * to traverse a node outside of a cycle doesn't.
   */
  @Test
  public void testGraphWithCycle() throws Exception {

    //     a --> b --> d -> e -> f
    //       <- c <-/

    ListMultimap<String, String> edges = LinkedListMultimap.create();
    edges.put("a", "b");
    edges.put("b", "d");
    edges.put("d", "c");
    edges.put("c", "a");
    edges.put("d", "e");
    edges.put("e", "f");

    ImportTraverser<String> traverser = new ImportTraverser<String>(s -> edges.get(s), CacheBuilder.newBuilder().build());

    try {
      List<String> traversal = traverser.traverseGraphRecursively("a");
      Assert.fail();
    } catch (CircularDependencyException cde) {
      // expected
    }

    try {
      List<String> traversal = traverser.traverseGraphRecursively("d");
      Assert.fail();
    } catch (CircularDependencyException cde) {
      // expected
    }

    List<String> traversal = traverser.traverseGraphRecursively("e");
    Assert.assertEquals(traversal, Lists.newArrayList("f"));
  }

}
