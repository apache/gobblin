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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.*;

public class BruteForceAllocatorTest {
  @Test
  public void testAllocateRequests()
      throws Exception {
    RequestAllocatorConfig<StringRequest> configuration =
        RequestAllocatorConfig.builder(new StringRequest.StringRequestEstimator())
            .withPrioritizer(new StringRequest.StringRequestComparator()).build();
    BruteForceAllocator<StringRequest> allocator =
        new BruteForceAllocator<>(configuration);

    ResourcePool pool = ResourcePool.builder().maxResource(StringRequest.MEMORY, 100.).build();

    List<Requestor<StringRequest>> requests = Lists.<Requestor<StringRequest>>newArrayList(
        new StringRequestor("r1", "a-50", "f-50", "k-20"),
        new StringRequestor("r2", "j-10", "b-20", "e-20"),
        new StringRequestor("r3", "g-20", "c-200", "d-30"));

    AllocatedRequestsIterator<StringRequest> result = allocator.allocateRequests(requests.iterator(), pool);

    List<StringRequest> resultList = Lists.newArrayList(result);

    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(resultList.get(0).getString(), "a-50");
    Assert.assertEquals(resultList.get(1).getString(), "b-20");
    // No c because it is too large to fit
    Assert.assertEquals(resultList.get(2).getString(), "d-30");
    Assert.assertEquals(resultList.get(3).getString(), "e-20");
  }

  @Test
  public void testThrowExceptionOnFailure() throws Exception {
    ResourceEstimator<StringRequest> failingEstimator = mock(ResourceEstimator.class);
    when(failingEstimator.estimateRequirement(any(), any())).thenThrow(new RuntimeException("Error"));

    RequestAllocatorConfig<StringRequest> configuration =
        RequestAllocatorConfig.builder(failingEstimator)
            .allowParallelization()
            .withPrioritizer(new StringRequest.StringRequestComparator()).build();
    BruteForceAllocator<StringRequest> allocator =
        new BruteForceAllocator<>(configuration);

    ResourcePool pool = ResourcePool.builder().maxResource(StringRequest.MEMORY, 100.).build();

    List<Requestor<StringRequest>> requests = Lists.<Requestor<StringRequest>>newArrayList(
        new StringRequestor("r1", "a-50", "f-50", "k-20"),
        new StringRequestor("r2", "j-10", "b-20", "e-20"),
        new StringRequestor("r3", "g-20", "c-200", "d-30"));

    Assert.expectThrows(RuntimeException.class, () -> allocator.allocateRequests(requests.iterator(), pool));
  }
}