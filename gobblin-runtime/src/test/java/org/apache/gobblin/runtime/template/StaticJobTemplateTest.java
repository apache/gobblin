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

package org.apache.gobblin.runtime.template;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;


public class StaticJobTemplateTest {

  @Test
  public void test() throws Exception {

    Map<String, String> confMap = Maps.newHashMap();
    confMap.put("key1", "value1");
    confMap.put(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST, "required1,required2");
    confMap.put(StaticJobTemplate.SUPER_TEMPLATE_KEY, "template2");

    JobCatalogWithTemplates catalog = Mockito.mock(JobCatalogWithTemplates.class);

    Mockito.when(catalog.getTemplate(new URI("template2"))).thenAnswer(
        new InheritingJobTemplateTest.TestTemplateAnswer(
            Lists.<URI>newArrayList(), ImmutableMap.of("key2", "value2"),
            Lists.<String>newArrayList(), catalog));

    StaticJobTemplate template =
        new StaticJobTemplate(new URI("template"), "1", "desc", ConfigFactory.parseMap(confMap), catalog);

    Assert.assertEquals(template.getSuperTemplates().size(), 1);
    Assert.assertEquals(template.getSuperTemplates().iterator().next().getUri(), new URI("template2"));

    Collection<String> required = template.getRequiredConfigList();
    Assert.assertEquals(required.size(), 2);
    Assert.assertTrue(required.contains("required1"));
    Assert.assertTrue(required.contains("required2"));

    Config rawTemplate = template.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("key1"), "value1");
    Assert.assertEquals(rawTemplate.getString("key2"), "value2");

    Config resolved = template.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required1", "r1", "required2", "r2")));
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("key2"), "value2");
    Assert.assertEquals(resolved.getString("required1"), "r1");
    Assert.assertEquals(resolved.getString("required2"), "r2");
  }

  @Test
  public void testMultipleTemplates() throws Exception {
    Map<String, String> confMap = Maps.newHashMap();
    confMap.put("key", "value");

    InheritingJobTemplateTest.TestTemplate
        template1 = new InheritingJobTemplateTest.TestTemplate(new URI("template1"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key1", "value1"),
        ImmutableList.of());
    InheritingJobTemplateTest.TestTemplate
        template2 = new InheritingJobTemplateTest.TestTemplate(new URI("template2"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key2", "value2"),
        ImmutableList.of());
    List<JobTemplate> templateList = new ArrayList<>();
    templateList.add(template1);
    templateList.add(template2);

    StaticJobTemplate template =
        new StaticJobTemplate(new URI("template"), "1", "desc", ConfigFactory.parseMap(confMap), templateList);
    Config resolved = template.getResolvedConfig(ConfigFactory.empty());
    Assert.assertEquals(resolved.getString("key"), "value");
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("key2"), "value2");
  }

  @Test
  public void testSecure() throws Exception {
    Map<String, Object> confMap = Maps.newHashMap();
    confMap.put("nonOverridableKey", "value1");
    confMap.put("overridableKey", "value1");
    confMap.put(StaticJobTemplate.IS_SECURE_KEY, true);
    confMap.put(StaticJobTemplate.SECURE_OVERRIDABLE_PROPERTIES_KEYS, "overridableKey, overridableKey2");

    StaticJobTemplate template = new StaticJobTemplate(URI.create("my://template"), "1", "desc", ConfigFactory.parseMap(confMap), (JobCatalogWithTemplates) null);

    Config userConfig = ConfigFactory.parseMap(ImmutableMap.of(
        "overridableKey", "override",
        "overridableKey2", "override2",
        "nonOverridableKey", "override",
        "somethingElse", "override"));
    Config resolved = template.getResolvedConfig(userConfig);

    Assert.assertEquals(resolved.entrySet().size(), 5);
    Assert.assertEquals(resolved.getString("nonOverridableKey"), "value1");
    Assert.assertEquals(resolved.getString("overridableKey"), "override");
    Assert.assertEquals(resolved.getString("overridableKey2"), "override2");
    Assert.assertFalse(resolved.hasPath("somethingElse"));

  }

}