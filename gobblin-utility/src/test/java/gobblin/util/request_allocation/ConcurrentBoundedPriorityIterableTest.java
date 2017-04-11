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

package gobblin.util.request_allocation;

import java.util.Comparator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


public class ConcurrentBoundedPriorityIterableTest {

  public static final String MEMORY = "memory";

  @Test
  public void test() throws Exception {

    ConcurrentBoundedPriorityIterable<String> iterable = new ConcurrentBoundedPriorityIterable<>(new MyComparator(),
        new MyEstimator(), ResourcePool.builder().maxResource(MEMORY, 100.).build());

    // doesn't fit
    Assert.assertFalse(iterable.add("a-500"));

    // add some elements until full
    Assert.assertTrue(iterable.add("d-50"));
    Assert.assertFalse(iterable.isFull());
    Assert.assertTrue(iterable.add("d-50"));
    Assert.assertTrue(iterable.isFull());

    // container full, cannot add low priority
    Assert.assertFalse(iterable.add("d-50"));
    Assert.assertFalse(iterable.add("e-50"));

    // can add item up to hard bound
    Assert.assertTrue(iterable.add("e-10"));

    // can add high priority item
    Assert.assertTrue(iterable.add("b-50"));

    // Check items
    List<String> items = Lists.newArrayList(Iterators.transform(iterable.iterator(),
        new AllocatedRequestsIteratorBase.TExtractor<String>()));
    Assert.assertEquals(items.size(), 2);
    Assert.assertEquals(items.get(0), "b-50");
    Assert.assertEquals(items.get(1), "d-50");

    iterable.reopen();
    // a high priority that won't fit even with evictions should not evict anything
    Assert.assertFalse(iterable.add("c-500"));
    items = Lists.newArrayList(Iterators.transform(iterable.iterator(),
        new AllocatedRequestsIteratorBase.TExtractor<String>()));
    Assert.assertEquals(items.size(), 2);

    iterable.reopen();
    // even if it is higher priority than everything else
    Assert.assertFalse(iterable.add("a-500"));
    items = Lists.newArrayList(Iterators.transform(iterable.iterator(),
        new AllocatedRequestsIteratorBase.TExtractor<String>()));
    Assert.assertEquals(items.size(), 2);
  }

  private class MyComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
      String o1CompareToken = o1.split("-")[0];
      String o2CompareToken = o2.split("-")[0];
      return o1CompareToken.compareTo(o2CompareToken);
    }
  }

  private class MyEstimator implements ResourceEstimator<String> {
    @Override
    public ResourceRequirement estimateRequirement(String s, ResourcePool resourcePool) {
      double memory = Double.parseDouble(s.split("-")[1]);
      return resourcePool.getResourceRequirementBuilder().setRequirement(MEMORY, memory).build();
    }
  }

}