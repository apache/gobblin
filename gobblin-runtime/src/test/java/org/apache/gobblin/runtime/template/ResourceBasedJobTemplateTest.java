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

package gobblin.runtime.template;

import java.util.Collection;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class ResourceBasedJobTemplateTest {

  @Test
  public void test() throws Exception {
    ResourceBasedJobTemplate template = ResourceBasedJobTemplate.forResourcePath("templates/test.template", null);

    Collection<String> required = template.getRequiredConfigList();
    Assert.assertEquals(required.size(), 3);
    Assert.assertTrue(required.contains("required0"));
    Assert.assertTrue(required.contains("required1"));
    Assert.assertTrue(required.contains("required2"));

    Config rawTemplate = template.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("templated0"), "x");
    Assert.assertEquals(rawTemplate.getString("templated1"), "y");

    Config resolved = template.getResolvedConfig(
        ConfigFactory.parseMap(ImmutableMap.of("required0", "r0", "required1", "r1", "required2", "r2")));
    Assert.assertEquals(resolved.getString("templated0"), "x");
    Assert.assertEquals(resolved.getString("required0"), "r0");
  }

}
