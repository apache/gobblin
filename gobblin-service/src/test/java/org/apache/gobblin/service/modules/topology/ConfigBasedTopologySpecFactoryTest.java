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

package org.apache.gobblin.service.modules.topology;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


public class ConfigBasedTopologySpecFactoryTest {

  private Config _config;
  private ConfigBasedTopologySpecFactory _configBasedTopologySpecFactory;

  @BeforeClass
  public void setup() throws Exception {
    String topology1 = "cluster1";
    String topology2 = "azkaban1";

    // Global properties
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TOPOLOGYSPEC_FACTORY_KEY, ConfigBasedTopologySpecFactory.class.getCanonicalName());
    properties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY, topology1 + "," + topology2);

    // Topology Cluster1 properties
    String topology1Prefix = ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX + topology1 + ".";
    properties.put(topology1Prefix + ServiceConfigKeys.TOPOLOGYSPEC_DESCRIPTION_KEY, "Topology for cluster");
    properties.put(topology1Prefix + ServiceConfigKeys.TOPOLOGYSPEC_VERSION_KEY, "1");
    properties.put(topology1Prefix + ServiceConfigKeys.TOPOLOGYSPEC_URI_KEY, "/mySpecs/" + topology1);
    properties.put(topology1Prefix + ServiceConfigKeys.SPEC_EXECUTOR_KEY,
        ServiceConfigKeys.DEFAULT_SPEC_EXECUTOR);
    properties.put(topology1Prefix + ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY, "salesforce:nosql");

    // Topology Azkaban1 properties
    String topology2Prefix = ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX + topology2 + ".";
    properties.put(topology2Prefix + ServiceConfigKeys.TOPOLOGYSPEC_DESCRIPTION_KEY, "Topology for Azkaban");
    properties.put(topology2Prefix + ServiceConfigKeys.TOPOLOGYSPEC_VERSION_KEY, "2");
    properties.put(topology2Prefix + ServiceConfigKeys.TOPOLOGYSPEC_URI_KEY, "/mySpecs/" + topology2);
    properties.put(topology2Prefix + ServiceConfigKeys.SPEC_EXECUTOR_KEY,
        ServiceConfigKeys.DEFAULT_SPEC_EXECUTOR);
    properties.put(topology2Prefix + ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY, "nosql:hdfs");

    _config = ConfigUtils.propertiesToConfig(properties);
    _configBasedTopologySpecFactory = new ConfigBasedTopologySpecFactory(_config);
  }

  @AfterClass
  public void cleanUp() throws Exception {
  }

  @Test
  public void testGetTopologies() {
    Collection<TopologySpec> topologySpecs = _configBasedTopologySpecFactory.getTopologies();
    Assert.assertTrue(topologySpecs.size() == 2, "Expected 2 topologies but received: " + topologySpecs.size());

    Iterator<TopologySpec> topologySpecIterator = topologySpecs.iterator();
    TopologySpec topologySpec1 = topologySpecIterator.next();
    Assert.assertTrue(topologySpec1.getDescription().equals("Topology for cluster"),
        "Description did not match with construction");
    Assert.assertTrue(topologySpec1.getVersion().equals("1"),
        "Version did not match with construction");

    TopologySpec topologySpec2 = topologySpecIterator.next();
    Assert.assertTrue(topologySpec2.getDescription().equals("Topology for Azkaban"),
        "Description did not match with construction");
    Assert.assertTrue(topologySpec2.getVersion().equals("2"),
        "Version did not match with construction");
  }

}