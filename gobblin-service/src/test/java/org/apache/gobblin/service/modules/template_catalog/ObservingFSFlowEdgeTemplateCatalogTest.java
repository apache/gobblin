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

package org.apache.gobblin.service.modules.template_catalog;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.testing.AssertWithBackoff;


@Slf4j
public class ObservingFSFlowEdgeTemplateCatalogTest {

  private File templateDir;
  private Config templateCatalogCfg;

  @BeforeClass
  public void setUp() throws Exception {
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    this.templateDir = Files.createTempDir();
    FileUtils.forceDeleteOnExit(templateDir);
    FileUtils.copyDirectory(new File(flowTemplateCatalogUri.getPath()), templateDir);
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, templateDir.toURI().toString());
    properties.put(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY, "1000");
    Config config = ConfigFactory.parseProperties(properties);
    this.templateCatalogCfg = config.withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
  }

  @Test
  public void testModifyFlowTemplate() throws Exception {
    ObservingFSFlowEdgeTemplateCatalog catalog = new ObservingFSFlowEdgeTemplateCatalog(this.templateCatalogCfg, new ReentrantReadWriteLock());
    ServiceManager serviceManager = new ServiceManager(Lists.newArrayList(catalog));
    serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);

    // Check cached flow template is returned
    FlowTemplate flowTemplate1 = catalog.getFlowTemplate(new URI(FSFlowTemplateCatalogTest.TEST_TEMPLATE_DIR_URI));
    FlowTemplate flowTemplate2 = catalog.getFlowTemplate(new URI(FSFlowTemplateCatalogTest.TEST_TEMPLATE_DIR_URI));
    Assert.assertSame(flowTemplate1, flowTemplate2);

    // Update a file flow catalog and check that the getFlowTemplate returns the new value
    Path flowConfPath = new File(new File(this.templateDir, FSFlowTemplateCatalogTest.TEST_TEMPLATE_NAME), "flow.conf").toPath();
    List<String> lines = java.nio.file.Files.readAllLines(flowConfPath);
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).equals("gobblin.flow.edge.input.dataset.descriptor.0.format=avro")) {
        lines.set(i, "gobblin.flow.edge.input.dataset.descriptor.0.format=any");
        break;
      }
    }
    java.nio.file.Files.write(flowConfPath, lines);

    Function testFunction = new GetFlowTemplateConfigFunction(new URI(FSFlowTemplateCatalogTest.TEST_TEMPLATE_DIR_URI), catalog,
        "gobblin.flow.edge.input.dataset.descriptor.0.format");
    AssertWithBackoff.create().timeoutMs(10000).assertEquals(testFunction, "any", "flow template updated");
  }

  @AllArgsConstructor
  private class GetFlowTemplateConfigFunction implements Function<Void, String> {
    private URI flowTemplateCatalogUri;
    private FSFlowTemplateCatalog flowTemplateCatalog;
    private String configKey;

    @Override
    public String apply(Void input) {
      try {
        return this.flowTemplateCatalog.getFlowTemplate(this.flowTemplateCatalogUri).getRawTemplateConfig().getString(this.configKey);
      } catch (SpecNotFoundException | JobTemplate.TemplateException | IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}