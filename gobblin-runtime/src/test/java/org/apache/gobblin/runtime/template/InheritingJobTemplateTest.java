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
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;

import lombok.AllArgsConstructor;


public class InheritingJobTemplateTest {


  @Test
  public void testSimpleInheritance() throws Exception {

    TestTemplate template1 = new TestTemplate(new URI("template1"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key1", "value1"),
        Lists.newArrayList("required"));
    TestTemplate template2 = new TestTemplate(new URI("template2"), Lists.<JobTemplate>newArrayList(template1), ImmutableMap.of("key2", "value2"),
        Lists.newArrayList("required2"));

    Collection<String> required = template2.getRequiredConfigList();
    Assert.assertEquals(required.size(), 2);
    Assert.assertTrue(required.contains("required"));
    Assert.assertTrue(required.contains("required2"));

    Config rawTemplate = template2.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("key1"), "value1");
    Assert.assertEquals(rawTemplate.getString("key2"), "value2");

    Config resolved = template2.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required", "r1", "required2", "r2")));
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("key2"), "value2");
    Assert.assertEquals(resolved.getString("required"), "r1");
    Assert.assertEquals(resolved.getString("required2"), "r2");

    try {
      // should throw exception because missing required property
      resolved = template2.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required", "r1")));
      Assert.fail();
    } catch (JobTemplate.TemplateException te) {
      // expected
    }
  }

  @Test
  public void testMultiInheritance() throws Exception {
    TestTemplate template1 = new TestTemplate(new URI("template1"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key1", "value1"),
        Lists.newArrayList("required"));
    TestTemplate template2 = new TestTemplate(new URI("template2"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key2", "value2"),
        Lists.newArrayList("required2"));
    TestTemplate inheriting = new TestTemplate(new URI("inheriting"), Lists.<JobTemplate>newArrayList(template1, template2), ImmutableMap.<String, String>of(),
        Lists.<String>newArrayList());

    Collection<String> required = inheriting.getRequiredConfigList();
    Assert.assertEquals(required.size(), 2);
    Assert.assertTrue(required.contains("required"));
    Assert.assertTrue(required.contains("required2"));

    Config rawTemplate = inheriting.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("key1"), "value1");
    Assert.assertEquals(rawTemplate.getString("key2"), "value2");

    Config resolved = inheriting.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required", "r1", "required2", "r2")));
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("key2"), "value2");
    Assert.assertEquals(resolved.getString("required"), "r1");
    Assert.assertEquals(resolved.getString("required2"), "r2");
  }

  @Test
  public void testLoopInheritance() throws Exception {

    JobCatalogWithTemplates catalog = Mockito.mock(JobCatalogWithTemplates.class);

    Mockito.when(catalog.getTemplate(new URI("template2"))).thenAnswer(
        new TestTemplateAnswer(Lists.newArrayList(new URI("template3")), ImmutableMap.of("key2", "value2"),
            Lists.<String>newArrayList(), catalog));
    Mockito.when(catalog.getTemplate(new URI("template3"))).thenAnswer(
        new TestTemplateAnswer(Lists.newArrayList(new URI("template1")), ImmutableMap.of("key3", "value3"),
            Lists.newArrayList("required3"), catalog));

    TestTemplate template = new TestTemplate(new URI("template1"), Lists.newArrayList(new URI("template2")),
        ImmutableMap.of("key1", "value1"), Lists.newArrayList("required"), catalog);

    Collection<String> required = template.getRequiredConfigList();
    Assert.assertEquals(required.size(), 2);
    Assert.assertTrue(required.contains("required"));
    Assert.assertTrue(required.contains("required3"));

    Config rawTemplate = template.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("key1"), "value1");
    Assert.assertEquals(rawTemplate.getString("key2"), "value2");
    Assert.assertEquals(rawTemplate.getString("key3"), "value3");

    Config resolved = template.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required", "r1", "required3", "r3")));
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("key2"), "value2");
    Assert.assertEquals(resolved.getString("key3"), "value3");
    Assert.assertEquals(resolved.getString("required"), "r1");
    Assert.assertEquals(resolved.getString("required3"), "r3");
  }

  @Test
  public void testSatisfySuperTemplateRequirements() throws Exception {
    TestTemplate template1 = new TestTemplate(new URI("template1"), Lists.<JobTemplate>newArrayList(), ImmutableMap.of("key1", "value1"),
        Lists.newArrayList("required"));
    TestTemplate template2 = new TestTemplate(new URI("template2"), Lists.<JobTemplate>newArrayList(template1), ImmutableMap.of("required", "r1"),
        Lists.newArrayList("required2"));

    Collection<String> required = template2.getRequiredConfigList();
    Assert.assertEquals(required.size(), 1);
    Assert.assertTrue(required.contains("required2"));

    Config rawTemplate = template2.getRawTemplateConfig();
    Assert.assertEquals(rawTemplate.getString("key1"), "value1");
    Assert.assertEquals(rawTemplate.getString("required"), "r1");

    Config resolved = template2.getResolvedConfig(ConfigFactory.parseMap(ImmutableMap.of("required2", "r2")));
    Assert.assertEquals(resolved.getString("key1"), "value1");
    Assert.assertEquals(resolved.getString("required"), "r1");
    Assert.assertEquals(resolved.getString("required2"), "r2");
  }

  @AllArgsConstructor
  public static class TestTemplateAnswer implements Answer<JobTemplate> {
    private final List<URI> superTemplateUris;
    private final Map<String,String> rawTemplate;
    private final List<String> required;
    private final JobCatalogWithTemplates catalog;

    @Override
    public JobTemplate answer(InvocationOnMock invocation)
        throws Throwable {
      return new TestTemplate((URI) invocation.getArguments()[0],
          this.superTemplateUris, this.rawTemplate, this.required, this.catalog);
    }
  }


  public static class TestTemplate extends InheritingJobTemplate {
    private final URI uri;
    private final Map<String,String> rawTemplate;
    private final List<String> required;

    public TestTemplate(URI uri, List<URI> superTemplateUris, Map<String, String> rawTemplate, List<String> required,
        JobCatalogWithTemplates catalog) throws SpecNotFoundException, TemplateException {
      super(superTemplateUris, catalog);
      this.uri = uri;
      this.rawTemplate = rawTemplate;
      this.required = required;
    }

    public TestTemplate(URI uri, List<JobTemplate> superTemplates, Map<String, String> rawTemplate, List<String> required) {
      super(superTemplates);
      this.uri = uri;
      this.rawTemplate = rawTemplate;
      this.required = required;
    }

    @Override
    public URI getUri() {
      return this.uri;
    }

    @Override
    public String getVersion() {
      return "1";
    }

    @Override
    public String getDescription() {
      return "description";
    }

    @Override
    protected Config getLocalRawTemplate() {
      return ConfigFactory.parseMap(this.rawTemplate);
    }

    @Override
    protected Collection<String> getLocallyRequiredConfigList() {
      return this.required;
    }

    @Override
    protected Config getLocallyResolvedConfig(Config userConfig) throws TemplateException {
      for (String required : this.required) {
        if (!userConfig.hasPath(required)) {
          throw new TemplateException("Missing required property " + required);
        }
      }
      return userConfig.withFallback(getLocalRawTemplate());
    }
  }

}