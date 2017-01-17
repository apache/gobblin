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
package gobblin.runtime.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/** Unit tests for {@link ClustersNames}. This test relies on the ClustersNames.properties file */
public class ClustersNamesTest {

  @Test
  public void testClustersNames() {
    ClustersNames clustersNames = ClustersNames.getInstance();
    Assert.assertEquals(clustersNames.getClusterName("http://cluster1-rm.some.company.com"),
                        "cluster1");
    Assert.assertEquals(clustersNames.getClusterName("http://cluster2-rm.some.company.com:12345"),
                        "cluster2");
    Assert.assertEquals(clustersNames.getClusterName("cluster1-rm.some.company.com"),
                        "cluster1-rm.some.company.com");
    Assert.assertEquals(clustersNames.getClusterName("http://cluster3-rm.some.company.com:12345"),
        "cluster3-rm.some.company.com");
    Assert.assertEquals(clustersNames.getClusterName("uri:fancy-uri"),
        "uri:fancy-uri");
  }
}
