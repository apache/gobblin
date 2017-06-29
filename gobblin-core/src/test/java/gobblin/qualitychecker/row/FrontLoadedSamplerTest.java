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

package gobblin.qualitychecker.row;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FrontLoadedSamplerTest {

  @Test
  public void test() {
    RowLevelPolicyChecker.FrontLoadedSampler sampler =
        new RowLevelPolicyChecker.FrontLoadedSampler(10, 2);

    List<Integer> sampled = Stream.iterate(0, i -> i++).limit(1000).filter(i -> sampler.acceptNext()).collect(Collectors.toList());
    Assert.assertTrue(sampled.size() >= 10);
    Assert.assertTrue(sampled.size() < 30);
    // check the first 10 values are the integers from 1 to 9
    Assert.assertEquals(sampled.subList(0, 10), Stream.iterate(0, i -> i++).limit(10).collect(Collectors.toList()));

    // with a very large decay factor, should have very few additional samples
    RowLevelPolicyChecker.FrontLoadedSampler sampler2 =
        new RowLevelPolicyChecker.FrontLoadedSampler(10, 10);
    sampled = Stream.iterate(0, i -> i++).limit(1000).filter(i -> sampler2.acceptNext()).collect(Collectors.toList());
    Assert.assertTrue(sampled.size() >= 10);
    Assert.assertTrue(sampled.size() < 15);
    // check the first 10 values are the integers from 1 to 9
    Assert.assertEquals(sampled.subList(0, 10), Stream.iterate(0, i -> i++).limit(10).collect(Collectors.toList()));

    // with a low decay factor, should have many additional samples
    RowLevelPolicyChecker.FrontLoadedSampler sampler3 =
        new RowLevelPolicyChecker.FrontLoadedSampler(10, 1.2);
    sampled = Stream.iterate(0, i -> i++).limit(1000).filter(i -> sampler3.acceptNext()).collect(Collectors.toList());
    Assert.assertTrue(sampled.size() >= 30);
    Assert.assertTrue(sampled.size() < 100);
    // check the first 10 values are the integers from 1 to 9
    Assert.assertEquals(sampled.subList(0, 10), Stream.iterate(0, i -> i++).limit(10).collect(Collectors.toList()));

  }

}
