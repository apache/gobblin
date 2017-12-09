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
package org.apache.gobblin.runtime.instance.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import avro.shaded.com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link HadoopConfigLoader}
 */
public class TestHadoopConfigLoader {

  @Test
  public void testOverride() {
    Config testConfig = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("c.d", "1")
        .put("e.f", "2")
        .put("hadoop-inject.a.b.c.ROOT", "2")
        .put("hadoop-inject.a.b.c.d", "3")
        .put("hadoop-inject.e.f", "4")
        .build());
    HadoopConfigLoader configLoader = new HadoopConfigLoader(testConfig);
    Configuration conf1 = configLoader.getConf();

    Assert.assertEquals(conf1.get("a.b.c"), "2");
    Assert.assertEquals(conf1.get("a.b.c.d"), "3");
    Assert.assertEquals(conf1.get("e.f"), "4");
    conf1.set("e.f", "5");
    Assert.assertEquals(conf1.get("e.f"), "5");

    Configuration conf2 = configLoader.getConf();

    Assert.assertEquals(conf2.get("a.b.c"), "2");
    Assert.assertEquals(conf2.get("a.b.c.d"), "3");
    Assert.assertEquals(conf2.get("e.f"), "4");
  }

}
