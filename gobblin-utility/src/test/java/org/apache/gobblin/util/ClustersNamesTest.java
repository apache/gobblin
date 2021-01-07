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

package org.apache.gobblin.util;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ClustersNames}. This test relies on the ClustersNames.properties file
 */
public class ClustersNamesTest {
    ClustersNames clustersNames = ClustersNames.getInstance();

    @Test
    public void testRegisteredUrls() {
        Assert.assertEquals(clustersNames.getClusterName("http://cluster1-rm.some.company.com"),
                "cluster1");
        Assert.assertEquals(clustersNames.getClusterName("http://cluster2-rm.some.company.com:12345"),
                "cluster2");
    }

    @Test
    public void testHostNameWithoutScheme() {
        Assert.assertEquals(clustersNames.getClusterName("cluster1-rm.some.company.com"),
                "cluster1-rm.some.company.com");
        Assert.assertEquals(clustersNames.getClusterName("cluster-host-name-4.some.company.com"),
                "cluster4");
    }

    @Test
    public void testUnregisteredUrl() {
        Assert.assertEquals(clustersNames.getClusterName("http://nonexistent-cluster-rm.some.company.com:12345"),
                "nonexistent-cluster-rm.some.company.com");
    }

    @Test
    public void testPortSpecificOverrides() {
        Assert.assertEquals(clustersNames.getClusterName("http://cluster-host-name-4.some.company.com/"),
                "cluster4");
        Assert.assertEquals(clustersNames.getClusterName("http://cluster-host-name-4.some.company.com:12345"),
                "cluster4");
        Assert.assertEquals(clustersNames.getClusterName("http://cluster-host-name-4.some.company.com:789"),
                "cluster4-custom-port");
    }

    @Test
    public void testLocalPaths() {
        Assert.assertEquals(clustersNames.getClusterName("file:///"), "localhost");
        Assert.assertEquals(clustersNames.getClusterName("file:/home/test"), "localhost");
    }

    @Test
    public void testEmptyNames() {
        Assert.assertEquals(clustersNames.getClusterName(""), "");
        Assert.assertNull(clustersNames.getClusterName((String) null));
    }

    @Test
    public void testInvalidUrls() {
        Assert.assertEquals(clustersNames.getClusterName("uri:fancy-uri"), "uri_fancy-uri");
        Assert.assertEquals(clustersNames.getClusterName("test/path"), "test_path");
        Assert.assertEquals(clustersNames.getClusterName("http://host/?s=^test"), "http___host__s__test");
    }
}
