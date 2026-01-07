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

package org.apache.gobblin.metrics.opentelemetry;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;


/**
 * Tests for {@link OpenTelemetryHelper} class.
 */
public class OpenTelemetryHelperTest {

  @Test
  public void testMergeAttributes() {
    Attributes emptyAttributes = Attributes.empty();
    Attributes attributes1 = Attributes.builder().put("key1", "value1").build();
    Attributes attributes2 = Attributes.builder().put("key2", "value2").build();

    Attributes attributes = OpenTelemetryHelper.mergeAttributes(null, emptyAttributes);
    Assert.assertEquals(attributes.size(), 0);
    Assert.assertEquals(attributes, emptyAttributes);

    attributes = OpenTelemetryHelper.mergeAttributes(attributes1, attributes2);
    Assert.assertEquals(attributes.size(), 2);
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("key1")), "value1");
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("key2")), "value2");

    attributes = OpenTelemetryHelper.mergeAttributes(attributes1, emptyAttributes);
    Assert.assertEquals(attributes.size(), 1);
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("key1")), "value1");
    Assert.assertNull(attributes.get(AttributeKey.stringKey("key2")));

  }

  @Test
  public void testToOpenTelemetryAttributes() {
    Attributes attributes = OpenTelemetryHelper.toOpenTelemetryAttributes(null);
    Assert.assertEquals(attributes.size(), 0);

    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");

    attributes = OpenTelemetryHelper.toOpenTelemetryAttributes(map);
    Assert.assertEquals(attributes.size(), 2);
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("key1")), "value1");
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("key2")), "value2");
  }

}
