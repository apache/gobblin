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

package gobblin.source.workunit;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link MultiWorkUnitWeightedQueue}.
 */
@Test(groups = {"gobblin.source.workunit"})
public class MultiWorkUnitWeightedQueueTest {

  /**
   * Test for {@link MultiWorkUnitWeightedQueue#MultiWorkUnitWeightedQueue()}. It adds a series of WorkUnits to an
   * instance of MultiWorkUnitWeightedQueue of checks the size of all the WorkUnits returned by
   * {@link MultiWorkUnitWeightedQueue#getQueueAsList()}.
   */
  @Test
  public void testDefaultConstructor() {
    int numWorkUnits = 10;
    int weight = 1;

    MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue = new MultiWorkUnitWeightedQueue();
    WorkUnit workUnit = WorkUnit.createEmpty();

    for (int i = 0; i < numWorkUnits; i++) {
      multiWorkUnitWeightedQueue.addWorkUnit(workUnit, weight);
    }

    List<WorkUnit> multiWorkUnitWeightedQueueList = multiWorkUnitWeightedQueue.getQueueAsList();
    Assert.assertEquals(multiWorkUnitWeightedQueueList.size(), numWorkUnits);

    MultiWorkUnit multiWorkUnit;
    for (WorkUnit workUnitElement : multiWorkUnitWeightedQueueList) {
      multiWorkUnit = (MultiWorkUnit) workUnitElement;
      Assert.assertEquals(multiWorkUnit.getWorkUnits().size(), 1);
    }
  }

  /**
   * Test for {@link MultiWorkUnitWeightedQueue#MultiWorkUnitWeightedQueue(int)}. It sets a limit on the maximum number
   * of MultiWorkUnits that can be created, adds a series of WorkUnits to the list, and checks the results of
   * {@link MultiWorkUnitWeightedQueue#getQueueAsList()} to ensure each MultiWorkUnit created is of proper length.
   */
  @Test
  public void testWithQueueSizeLimit() {
    int maxMultiWorkUnits = 10;
    int numWorkUnits = 100;
    int weight = 1;

    MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue = new MultiWorkUnitWeightedQueue(maxMultiWorkUnits);
    WorkUnit workUnit = WorkUnit.createEmpty();

    for (int i = 0; i < numWorkUnits; i++) {
      multiWorkUnitWeightedQueue.addWorkUnit(workUnit, weight);
    }

    MultiWorkUnit multiWorkUnit;
    for (WorkUnit workUnitElement :  multiWorkUnitWeightedQueue.getQueueAsList()) {
      multiWorkUnit = (MultiWorkUnit) workUnitElement;
      Assert.assertEquals(multiWorkUnit.getWorkUnits().size(), numWorkUnits / maxMultiWorkUnits);
    }
  }
}
