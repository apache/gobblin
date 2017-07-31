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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Properties;

import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import org.apache.gobblin.runtime.template.HOCONInputStreamJobTemplate;
import org.apache.gobblin.runtime.template.ResourceBasedJobTemplate;


/**
 * Testing the functions for reading template merging template with user-specified attributes.
 * 1. Reading the template configuration, testing size or something
 * 2. Testing the required attributes result.
 */
@Test(groups = {"gobblin.runtime"})
public class TemplateTest {

  // For template inside resource folder.
  private File jobConfigDir;
  private Properties userProp ;

  @BeforeClass
  public void setUp()
      throws IOException, URISyntaxException {

    // Creating userCustomized stuff
    this.jobConfigDir =
        Files.createTempDirectory(String.format("gobblin-test_%s_job-conf", this.getClass().getSimpleName())).toFile();
    FileUtils.forceDeleteOnExit(this.jobConfigDir);

    //User specified file content:
    this.userProp = new Properties();
    userProp.setProperty("a", "1");
    userProp.setProperty("templated0", "2");
    userProp.setProperty("required0", "r0");
    userProp.setProperty("required1", "r1");
    userProp.setProperty("required2", "r2");
    userProp.setProperty("job.template", "templates/test.template");

    // User specified file's name : /[jobConfigDirName]/user.attr
    userProp.store(new FileWriter(new File(this.jobConfigDir, "user.attr")), "");
  }

  @Test
  public void testRequiredAttrList() throws Exception {
    Properties jobProps = this.userProp;

    Collection<String> requiredConfigList = ResourceBasedJobTemplate.forURI(new URI(
        jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH)), new PackagedTemplatesJobCatalogDecorator())
        .getRequiredConfigList();
    Assert.assertEquals(requiredConfigList.size(), 3);
    Assert.assertTrue( requiredConfigList.contains("required0"));
    Assert.assertTrue( requiredConfigList.contains("required1"));
    Assert.assertTrue( requiredConfigList.contains("required2"));
  }

  // Testing the resolving of userCustomized attributes and template is correct.
  @Test
  public void testResolvingConfig()
      throws Exception {
    Config jobProps = ConfigFactory.parseProperties(this.userProp);
    Assert.assertEquals(ConfigUtils.configToProperties(jobProps).size(), 6);
    jobProps = ResourceBasedJobTemplate.forResourcePath(jobProps.getString(ConfigurationKeys.JOB_TEMPLATE_PATH),
        new PackagedTemplatesJobCatalogDecorator())
        .getResolvedConfig(jobProps);
    // Remove job.template in userSpecified file and gobblin.template.required_attributes in template
    Assert.assertEquals(ConfigUtils.configToProperties(jobProps).size(), 8);

    Properties targetResolvedJobProps = new Properties() ;
    targetResolvedJobProps.setProperty("a", "1");
    targetResolvedJobProps.setProperty("templated0", "2");
    targetResolvedJobProps.setProperty("templated1", "y");
    targetResolvedJobProps.setProperty("required0","r0");
    targetResolvedJobProps.setProperty("required1","r1");
    targetResolvedJobProps.setProperty("required2","r2");
    targetResolvedJobProps.setProperty(ConfigurationKeys.JOB_TEMPLATE_PATH, "templates/test.template");
    targetResolvedJobProps.setProperty(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST, "required0,required1,required2");

    Assert.assertEquals(targetResolvedJobProps, ConfigUtils.configToProperties(jobProps));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (this.jobConfigDir != null) {
      FileUtils.forceDelete(this.jobConfigDir);
    }
  }
}
