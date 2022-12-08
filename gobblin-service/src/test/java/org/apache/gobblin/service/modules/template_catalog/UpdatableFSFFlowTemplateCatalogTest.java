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
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.template.FlowTemplate;


@Slf4j
public class UpdatableFSFFlowTemplateCatalogTest {

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
    Config config = ConfigFactory.parseProperties(properties);
    this.templateCatalogCfg = config.withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
  }

  @Test
  public void testModifyFlowTemplate() throws Exception {
    UpdatableFSFlowTemplateCatalog catalog = new UpdatableFSFlowTemplateCatalog(this.templateCatalogCfg, new ReentrantReadWriteLock());

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
    catalog.clearTemplates();
    Assert.assertEquals(catalog.getFlowTemplate(new URI(FSFlowTemplateCatalogTest.TEST_TEMPLATE_DIR_URI)).
            getRawTemplateConfig().getString("gobblin.flow.edge.input.dataset.descriptor.0.format"), "any");
  }
}