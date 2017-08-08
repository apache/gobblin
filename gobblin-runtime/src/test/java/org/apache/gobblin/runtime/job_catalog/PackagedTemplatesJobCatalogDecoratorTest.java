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

package org.apache.gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.Collection;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.template.ResourceBasedJobTemplate;


public class PackagedTemplatesJobCatalogDecoratorTest {

  @Test
  public void test() throws Exception {

    JobCatalogWithTemplates underlying = Mockito.mock(JobCatalogWithTemplates.class);

    JobCatalogWithTemplates catalog = new PackagedTemplatesJobCatalogDecorator(underlying);

    JobTemplate classTemplate =
        catalog.getTemplate(new URI(PackagedTemplatesJobCatalogDecorator.CLASS + "://" + TestTemplate.class.getName()));
    Assert.assertEquals(classTemplate.getClass(), TestTemplate.class);

    try {
      catalog.getTemplate(new URI(PackagedTemplatesJobCatalogDecorator.CLASS + "://" + "non.existing.class"));
      Assert.fail();
    } catch (SpecNotFoundException exc) {
      // expect exception
    }

    JobTemplate resourceTemplate =
        catalog.getTemplate(new URI(PackagedTemplatesJobCatalogDecorator.RESOURCE + ":///templates/test.template"));
    Assert.assertEquals(resourceTemplate.getClass(), ResourceBasedJobTemplate.class);
    Assert.assertEquals(resourceTemplate.getRequiredConfigList().size(), 3);

    URI uri = new URI("scheme:///templates/test.template");
    try {
      catalog.getTemplate(uri);
      Assert.fail();
    } catch (SpecNotFoundException exc) {
      // expect exception
    }
    Mockito.verify(underlying).getTemplate(uri);
  }

  public static class TestTemplate implements JobTemplate {

    @Override
    public URI getUri() {
      return null;
    }

    @Override
    public String getVersion() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public Config getRawTemplateConfig() {
      return null;
    }

    @Override
    public Collection<String> getRequiredConfigList() {
      return null;
    }

    @Override
    public Config getResolvedConfig(Config userConfig) {
      return null;
    }
  }

}
