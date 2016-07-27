/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
/**
 * Testing the functions for reading template merging template with user-specified attributes.
 * 1. Reading the template configuration, testing size or something
 * 2. Testing the required attributes result.
 */
@Test(groups = {"gobblin.runtime"})
public class TemplateTest {

  // For template inside resource folder.
  private File userCustomizedFile;
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
    userProp.setProperty("b", "2");
    userProp.setProperty("job.template", "templates/test.template");

    // User specified file's name : /[jobConfigDirName]/user.attr
    userProp.store(new FileWriter(new File(this.jobConfigDir, "user.attr")), "");
  }

  @Test
  public void testRequiredAttrList() {
    Properties jobProps = this.userProp;

    List<String> requiredConfigList = (new SimpleGeneralJobTemplate(
        jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH))).getRequiredConfigList();
    Assert.assertEquals(requiredConfigList.size(), 3);
    Assert.assertEquals(requiredConfigList.get(0), "required0");
    Assert.assertEquals(requiredConfigList.get(1), "required1");
    Assert.assertEquals(requiredConfigList.get(2), "required2");
  }

  // Testing the resolving of userCustomized attributes and template is correct.
  @Test
  public void testResolvingConfig()
      throws IOException {
    Properties jobProps = this.userProp ;
    Assert.assertEquals(jobProps.size(), 3);
    jobProps = (new SimpleGeneralJobTemplate(
        jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH))).getResolvedConfigAsProperties(jobProps);
    // Remove job.template in userSpecified file and required.attribute in template
    Assert.assertEquals(jobProps.size(), 5);

    Properties targetResolvedJobProps = new Properties() ;
    targetResolvedJobProps.setProperty("a", "1");
    targetResolvedJobProps.setProperty("b", "2");
    targetResolvedJobProps.setProperty("attr1","x");
    targetResolvedJobProps.setProperty("attr2","y");
    targetResolvedJobProps.setProperty("attr3","z");

    Assert.assertEquals(targetResolvedJobProps, jobProps);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (this.jobConfigDir != null) {
      FileUtils.forceDelete(this.jobConfigDir);
    }
  }
}
