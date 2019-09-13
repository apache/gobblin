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
package org.apache.gobblin.cluster;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class ContainerHealthMetricsServiceTest {

  @Test
  public void testRunOneIteration() throws Exception {
    Config config = ConfigFactory.empty();
    ContainerHealthMetricsService service = new ContainerHealthMetricsService(config);
    service.runOneIteration();
    long processCpuTime1 = service.processCpuTime.get();
    Thread.sleep(10);
    service.runOneIteration();
    long processCpuTime2 = service.processCpuTime.get();
    Assert.assertTrue( processCpuTime1 <= processCpuTime2);
  }
}
