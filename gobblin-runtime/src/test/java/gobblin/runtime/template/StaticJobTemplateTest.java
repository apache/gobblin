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

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobCatalogWithTemplates;


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

}