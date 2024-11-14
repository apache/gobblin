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

package org.apache.gobblin;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.util.AzkabanLauncherUtils;


@Test
public class AzkabanLauncherUtilsTest {

  @Test
  public void testPropertyPlaceholderReplacement() {
    Properties props = new Properties();

    props.setProperty("placeholderMap", ":emptyStringPlaceholder, :spacePlaceholder,\\t:tabPlaceholder");
    props.setProperty("key1", "emptyStringPlaceholder");
    props.setProperty("key2", "spacePlaceholder");
    props.setProperty("key3", "tabPlaceholder");
    props.setProperty("key4", "someOtherValue");
    props.setProperty("key5", "123emptyStringPlaceholder");

    props = AzkabanLauncherUtils.undoPlaceholderConversion(props);
    Assert.assertEquals("", props.get("key1").toString());
    Assert.assertEquals(" ", props.get("key2").toString());
    Assert.assertEquals("\\t", props.get("key3").toString());
    Assert.assertEquals("someOtherValue", props.get("key4").toString());

    // should replace exact matches only
    Assert.assertEquals("123emptyStringPlaceholder", props.get("key5").toString());
  }
}
