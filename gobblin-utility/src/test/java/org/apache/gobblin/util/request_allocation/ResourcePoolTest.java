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

package org.apache.gobblin.util.request_allocation;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ResourcePoolTest {

  public static final String MEMORY = "Memory";
  public static final String TIME = "Time";

  @Test
  public void test() {

    ResourcePool pool = ResourcePool.builder().maxResource(MEMORY, 1000.).maxResource(TIME, 200.).tolerance(MEMORY, 2.)
        .defaultRequirement(TIME, 1.).build();

    Assert.assertEquals(pool.getNumDimensions(), 2);
    Assert.assertEquals(pool.getSoftBound(), new double[]{1000, 200});
    // Default tolerance is 1.2
    Assert.assertEquals(pool.getHardBound(), new double[]{2000, 240});
    // Test default resource use
    Assert.assertEquals(pool.getResourceRequirementBuilder().build().getResourceVector(), new double[]{0, 1});

    ResourceRequirement requirement = pool.getResourceRequirementBuilder().setRequirement(MEMORY, 10.).build();
    Assert.assertEquals(requirement.getResourceVector(), new double[]{10, 1});
  }

}