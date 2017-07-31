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

package org.apache.gobblin.ingestion.google.webmaster;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class SimpleProducerJobTest {
  @Test
  public void testNotDivisibleJobs() {
    ProducerJob job1 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-22");
    Assert.assertTrue(job1.partitionJobs().isEmpty());

    ProducerJob job2 = new SimpleProducerJob("p1", "2016-11-23", "2016-11-22");
    Assert.assertTrue(job2.partitionJobs().isEmpty());
  }

  @Test
  public void testDivisibleJobs1() {
    ProducerJob job3 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-23");
    List<? extends ProducerJob> divides = job3.partitionJobs();
    Assert.assertEquals(divides.size(), 2);
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-22", "2016-11-22"), divides.get(0));
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-23", "2016-11-23"), divides.get(1));
  }

  @Test
  public void testDivisibleJobs2() {
    ProducerJob job3 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-24");
    List<? extends ProducerJob> divides = job3.partitionJobs();
    Assert.assertEquals(divides.size(), 2);
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-22", "2016-11-23"), divides.get(0));
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-24", "2016-11-24"), divides.get(1));
  }
}
