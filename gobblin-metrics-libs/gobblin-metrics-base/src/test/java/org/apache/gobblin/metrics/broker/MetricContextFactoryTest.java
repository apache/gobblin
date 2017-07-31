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

package org.apache.gobblin.metrics.broker;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.BrokerConfigurationKeyGenerator;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;


public class MetricContextFactoryTest {

  @Test
  public void test() throws Exception {

    MetricContextFactory<SimpleScopeType> factory = new MetricContextFactory<>();

    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        BrokerConfigurationKeyGenerator.generateKey(factory, null, null, MetricContextFactory.TAG_KEY + ".tag1"), "value1",
        BrokerConfigurationKeyGenerator.generateKey(factory, null, SimpleScopeType.GLOBAL, MetricContextFactory.TAG_KEY + ".tag2"), "value2",
        BrokerConfigurationKeyGenerator.generateKey(factory, null, SimpleScopeType.LOCAL, MetricContextFactory.TAG_KEY + ".tag3"), "value3"
    ));

    SharedResourcesBroker<SimpleScopeType> rootBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        SimpleScopeType.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<SimpleScopeType> localBroker = rootBroker.newSubscopedBuilder(SimpleScopeType.LOCAL.defaultScopeInstance()).build();

    MetricContext localContext = localBroker.getSharedResource(factory, new MetricContextKey());

    Map<String, String> tagMap = (Map<String, String>) Tag.toMap(Tag.tagValuesToString(localContext.getTags()));
    Assert.assertEquals(tagMap.get("tag1"), "value1");
    Assert.assertEquals(tagMap.get("tag2"), "value2");
    Assert.assertEquals(tagMap.get("tag3"), "value3");

    MetricContext globalContext = rootBroker.getSharedResource(factory, new MetricContextKey());
    Assert.assertEquals(localContext.getParent().get(), globalContext);
    tagMap = (Map<String, String>) Tag.toMap(Tag.tagValuesToString(globalContext.getTags()));
    Assert.assertEquals(tagMap.get("tag1"), "value1");
    Assert.assertEquals(tagMap.get("tag2"), "value2");
    Assert.assertFalse(tagMap.containsKey("tag3"));
  }

  @Test
  public void testSubTaggedMetricContext() throws Exception {
    MetricContextFactory<SimpleScopeType> factory = new MetricContextFactory<>();

    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        BrokerConfigurationKeyGenerator.generateKey(factory, null, null, MetricContextFactory.TAG_KEY + ".tag1"), "value1"
    ));

    SharedResourcesBroker<SimpleScopeType> rootBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        SimpleScopeType.GLOBAL.defaultScopeInstance());

    MetricContext metricContext = rootBroker.getSharedResource(factory,
        new SubTaggedMetricContextKey("myMetricContext", ImmutableMap.of("tag2", "value2")));

    Map<String, String> tagMap = (Map<String, String>) Tag.toMap(Tag.tagValuesToString(metricContext.getTags()));
    Assert.assertEquals(metricContext.getName(), "myMetricContext");
    Assert.assertEquals(tagMap.get("tag1"), "value1");
    Assert.assertEquals(tagMap.get("tag2"), "value2");

    MetricContext metricContext2 = rootBroker.getSharedResource(factory,
        new SubTaggedMetricContextKey("myMetricContext", ImmutableMap.of("tag2", "value2")));
    Assert.assertEquals(metricContext, metricContext2);

    MetricContext metricContext3 = rootBroker.getSharedResource(factory,
        new SubTaggedMetricContextKey("myMetricContext", ImmutableMap.of("tag3", "value3")));
    Assert.assertNotEquals(metricContext, metricContext3);

    MetricContext parent = rootBroker.getSharedResource(factory, new MetricContextKey());
    tagMap = (Map<String, String>) Tag.toMap(Tag.tagValuesToString(parent.getTags()));
    Assert.assertEquals(metricContext.getParent().get(), parent);
    Assert.assertEquals(tagMap.get("tag1"), "value1");
    Assert.assertFalse(tagMap.containsKey("tag2"));
  }

}
