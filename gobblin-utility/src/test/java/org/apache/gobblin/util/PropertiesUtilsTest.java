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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


public class PropertiesUtilsTest {

  @Test
  public void testExtractPropertiesWithPrefix() {

    Properties properties = new Properties();
    properties.setProperty("k1.kk1", "v1");
    properties.setProperty("k1.kk2", "v2");
    properties.setProperty("k2.kk", "v3");

    // First prefix
    Properties extractedPropertiesK1 = PropertiesUtils.extractPropertiesWithPrefix(properties, Optional.of("k1"));
    Assert.assertEquals(extractedPropertiesK1.getProperty("k1.kk1"), "v1");
    Assert.assertEquals(extractedPropertiesK1.getProperty("k1.kk2"), "v2");
    Assert.assertTrue(!extractedPropertiesK1.containsKey("k2.kk"));

    // Second prefix
    Properties extractedPropertiesK2 = PropertiesUtils.extractPropertiesWithPrefix(properties, Optional.of("k2"));
    Assert.assertTrue(!extractedPropertiesK2.containsKey("k1.kk1"));
    Assert.assertTrue(!extractedPropertiesK2.containsKey("k1.kk2"));
    Assert.assertEquals(extractedPropertiesK2.getProperty("k2.kk"), "v3");

    // Missing prefix
    Properties extractedPropertiesK3 = PropertiesUtils.extractPropertiesWithPrefix(properties, Optional.of("k3"));
    Assert.assertTrue(!extractedPropertiesK3.containsKey("k1.kk1"));
    Assert.assertTrue(!extractedPropertiesK3.containsKey("k1.kk1"));
    Assert.assertTrue(!extractedPropertiesK3.containsKey("k2.kk"));
  }
}
