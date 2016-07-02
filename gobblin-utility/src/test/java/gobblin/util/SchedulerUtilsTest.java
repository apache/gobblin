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
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;


/**
 * Unit tests for {@link SchedulerUtils}.
 */
@Test(groups = {"gobblin.util"})
public class SchedulerUtilsTest {

  // For general type of File system

  private File jobConfigDir;
  private File subDir1;
  private File subDir11;
  private File subDir2;

  @BeforeClass
  public void setUp()
      throws IOException {
    this.jobConfigDir = java.nio.file.Files.createTempDirectory(
        String.format("gobblin-test_%s_job-conf", this.getClass().getSimpleName())).toFile();
    FileUtils.forceDeleteOnExit(this.jobConfigDir);
    this.subDir1 = new File(this.jobConfigDir, "test1");
    this.subDir11 = new File(this.subDir1, "test11");
    this.subDir2 = new File(this.jobConfigDir, "test2");

    this.subDir1.mkdirs();
    this.subDir11.mkdirs();
    this.subDir2.mkdirs();

    Properties rootProps = new Properties();
    rootProps.setProperty("k1", "a1");
    rootProps.setProperty("k2", "a2");
    // test-job-conf-dir/root.properties
    rootProps.store(new FileWriter(new File(this.jobConfigDir, "root.properties")), "");

    Properties props1 = new Properties();
    props1.setProperty("k1", "b1");
    props1.setProperty("k3", "a3");
    // test-job-conf-dir/test1/test.properties
    props1.store(new FileWriter(new File(this.subDir1, "test.properties")), "");

    Properties jobProps1 = new Properties();
    jobProps1.setProperty("k1", "c1");
    jobProps1.setProperty("k3", "b3");
    jobProps1.setProperty("k6", "a6");
    // test-job-conf-dir/test1/test11.pull
    jobProps1.store(new FileWriter(new File(this.subDir1, "test11.pull")), "");

    Properties jobProps2 = new Properties();
    jobProps2.setProperty("k7", "a7");
    // test-job-conf-dir/test1/test12.PULL
    jobProps2.store(new FileWriter(new File(this.subDir1, "test12.PULL")), "");

    Properties jobProps3 = new Properties();
    jobProps3.setProperty("k1", "d1");
    jobProps3.setProperty("k8", "a8");
    jobProps3.setProperty("k9", "${k8}");
    // test-job-conf-dir/test1/test11/test111.pull
    jobProps3.store(new FileWriter(new File(this.subDir11, "test111.pull")), "");

    Properties props2 = new Properties();
    props2.setProperty("k2", "b2");
    props2.setProperty("k5", "a5");
    // test-job-conf-dir/test2/test.properties
    props2.store(new FileWriter(new File(this.subDir2, "test.PROPERTIES")), "");

    Properties jobProps4 = new Properties();
    jobProps4.setProperty("k5", "b5");
    // test-job-conf-dir/test2/test21.PULL
    jobProps4.store(new FileWriter(new File(this.subDir2, "test21.PULL")), "");
  }

  @Test
  public void testLoadJobConfigs()
      throws ConfigurationException, IOException {
    Properties properties = new Properties();
    System.err.println(" LEI :  " + this.jobConfigDir.getAbsolutePath() );
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, this.jobConfigDir.getAbsolutePath());
    List<Properties> jobConfigs = SchedulerUtils.loadGenericJobConfigs(properties);
    Assert.assertEquals(jobConfigs.size(), 4);

    // test-job-conf-dir/test1/test11/test111.pull
    Properties jobProps1 = getJobConfigForFile(jobConfigs, "test111.pull");
    Assert.assertEquals(jobProps1.stringPropertyNames().size(), 7);
    Assert.assertTrue(jobProps1.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY) || jobProps1.containsKey(
        ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    Assert.assertTrue(jobProps1.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps1.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps1.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps1.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps1.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps1.getProperty("k9"), "a8");

    // test-job-conf-dir/test1/test11.pull
    Properties jobProps2 = getJobConfigForFile(jobConfigs, "test11.pull");
    Assert.assertEquals(jobProps2.stringPropertyNames().size(), 6);
    Assert.assertTrue(jobProps2.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY) || jobProps1.containsKey(
        ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    Assert.assertTrue(jobProps2.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps2.getProperty("k1"), "c1");
    Assert.assertEquals(jobProps2.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps2.getProperty("k3"), "b3");
    Assert.assertEquals(jobProps2.getProperty("k6"), "a6");

    // test-job-conf-dir/test1/test12.PULL
    Properties jobProps3 = getJobConfigForFile(jobConfigs, "test12.PULL");
    Assert.assertEquals(jobProps3.stringPropertyNames().size(), 6);
    Assert.assertTrue(jobProps3.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY) || jobProps1.containsKey(
        ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    Assert.assertTrue(jobProps3.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps3.getProperty("k1"), "b1");
    Assert.assertEquals(jobProps3.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps3.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps3.getProperty("k7"), "a7");

    // test-job-conf-dir/test2/test21.PULL
    Properties jobProps4 = getJobConfigForFile(jobConfigs, "test21.PULL");
    Assert.assertEquals(jobProps4.stringPropertyNames().size(), 5);
    Assert.assertTrue(jobProps4.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY) || jobProps1.containsKey(
        ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
    Assert.assertTrue(jobProps4.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps4.getProperty("k1"), "a1");
    Assert.assertEquals(jobProps4.getProperty("k2"), "b2");
    Assert.assertEquals(jobProps4.getProperty("k5"), "b5");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (this.jobConfigDir != null) {
      FileUtils.forceDelete(this.jobConfigDir);
    }
  }

  private Properties getJobConfigForFile(List<Properties> jobConfigs, String fileName) {
    for (Properties jobConfig : jobConfigs) {
      if (jobConfig.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY).endsWith(fileName)) {
        return jobConfig;
      }
    }
    return null;
  }
}
