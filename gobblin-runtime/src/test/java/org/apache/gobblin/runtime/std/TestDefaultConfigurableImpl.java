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

package org.apache.gobblin.runtime.std;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.runtime.std.DefaultConfigurableImpl;

/**
 * Unit tests for {@link DefaultConfigurableImpl}
 */
public class TestDefaultConfigurableImpl {

  @Test
  public void testFromProperties() {
    Properties props = new Properties();
    props.put("a1", "a_value");
    props.put("a2.b", "1");
    props.put("a2.c.d", "12.34");
    props.put("a2.c.d2", "true");

    DefaultConfigurableImpl c = DefaultConfigurableImpl.createFromProperties(props);
    Assert.assertEquals(c.getConfig().getString("a1"), "a_value");
    Assert.assertEquals(c.getConfig().getLong("a2.b"), 1L);
    Assert.assertEquals(c.getConfig().getDouble("a2.c.d"), 12.34);
    Assert.assertTrue(c.getConfig().getBoolean("a2.c.d2"));
  }

  @Test
  public void testFromConfig() {
    Config cfg =
        ConfigFactory.empty()
                     .withValue("a1", ConfigValueFactory.fromAnyRef("some_string"))
                     .withValue("a2.b", ConfigValueFactory.fromAnyRef(-1))
                     .withValue("a2.c.d", ConfigValueFactory.fromAnyRef(1.2))
                     .withValue("a2.e.f", ConfigValueFactory.fromAnyRef(true));
    DefaultConfigurableImpl c = DefaultConfigurableImpl.createFromConfig(cfg);
    Assert.assertEquals(c.getConfigAsProperties().getProperty("a1"), "some_string");
    Assert.assertEquals(c.getConfigAsProperties().getProperty("a2.b"), "-1");
    Assert.assertEquals(c.getConfigAsProperties().getProperty("a2.c.d"), "1.2");
    Assert.assertEquals(c.getConfigAsProperties().getProperty("a2.e.f"), "true");
  }

}
