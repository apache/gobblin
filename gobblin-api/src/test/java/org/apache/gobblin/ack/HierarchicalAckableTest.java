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
package org.apache.gobblin.ack;

import org.testng.Assert;
import org.testng.TestException;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class HierarchicalAckableTest {

  @Test
  public void testCloseBeforeAck() throws Exception {
    BasicAckableForTesting ackable = new BasicAckableForTesting();
    HierarchicalAckable hierarchicalAckable = new HierarchicalAckable(Lists.newArrayList(ackable));

    Ackable child1 = hierarchicalAckable.newChildAckable();
    Ackable child2 = hierarchicalAckable.newChildAckable();

    hierarchicalAckable.close();

    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    child2.ack();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    // acking same child twice does not ack parent
    child2.ack();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    child1.ack();
    Assert.assertEquals(ackable.acked, 1);
    Assert.assertEquals(ackable.nacked, 0);

    // Acking again changes nothing
    child1.ack();
    Assert.assertEquals(ackable.acked, 1);
    Assert.assertEquals(ackable.nacked, 0);
  }

  @Test
  public void testAckBeforeClose() throws Exception {
    BasicAckableForTesting ackable = new BasicAckableForTesting();
    HierarchicalAckable hierarchicalAckable = new HierarchicalAckable(Lists.newArrayList(ackable));

    Ackable child1 = hierarchicalAckable.newChildAckable();
    Ackable child2 = hierarchicalAckable.newChildAckable();

    child2.ack();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    child1.ack();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    hierarchicalAckable.close();
    Assert.assertEquals(ackable.acked, 1);
    Assert.assertEquals(ackable.nacked, 0);
  }

  @Test
  public void testChildNacked() throws Exception {
    BasicAckableForTesting ackable = new BasicAckableForTesting();
    HierarchicalAckable hierarchicalAckable = new HierarchicalAckable(Lists.newArrayList(ackable));

    Ackable child1 = hierarchicalAckable.newChildAckable();
    Ackable child2 = hierarchicalAckable.newChildAckable();

    child2.ack();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    hierarchicalAckable.close();
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 0);

    child1.nack(new TestException("test"));
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(ackable.nacked, 1);

    Assert.assertNotNull(ackable.throwable);
    Assert.assertTrue(ackable.throwable instanceof HierarchicalAckable.ChildrenFailedException);
    Assert.assertEquals(((HierarchicalAckable.ChildrenFailedException) ackable.throwable).getFailureCauses().size(), 1);
  }

  @Test
  public void testMultipleParents() throws Exception {
    BasicAckableForTesting ackable1 = new BasicAckableForTesting();
    BasicAckableForTesting ackable2 = new BasicAckableForTesting();

    HierarchicalAckable hierarchicalAckable = new HierarchicalAckable(Lists.newArrayList(ackable1, ackable2));

    Ackable child1 = hierarchicalAckable.newChildAckable();

    hierarchicalAckable.close();
    child1.ack();

    Assert.assertEquals(ackable1.acked, 1);
    Assert.assertEquals(ackable2.acked, 1);
  }
}
