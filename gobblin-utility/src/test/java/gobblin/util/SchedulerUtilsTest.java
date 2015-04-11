/* (c) 2014 LinkedIn Corp. All rights reserved.
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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.hadoop.fs.FileUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;


/**
 * Unit tests for {@link SchedulerUtils}.
 */
@Test(groups = {"gobblin.util"})
public class SchedulerUtilsTest {

  private static final File JOB_CONF_ROOT_DIR = new File("gobblin-test/test-job-conf-dir");
  private static final File SUB_DIR1 = new File(JOB_CONF_ROOT_DIR, "test1");
  private static final File SUB_DIR11 = new File(SUB_DIR1, "test11");
  private static final File SUB_DIR2 = new File(JOB_CONF_ROOT_DIR, "test2");

  @BeforeClass
  public void setUp()
      throws IOException {
    SUB_DIR1.mkdirs();
    SUB_DIR11.mkdirs();
    SUB_DIR2.mkdirs();

    Properties rootProps = new Properties();
    rootProps.setProperty("k1", "a1");
    rootProps.setProperty("k2", "a2");
    // test-job-conf-dir/root.properties
    rootProps.store(new FileWriter(new File(JOB_CONF_ROOT_DIR, "root.properties")), "");

    Properties props1 = new Properties();
    props1.setProperty("k1", "b1");
    props1.setProperty("k3", "a3");
    // test-job-conf-dir/test1/test.properties
    props1.store(new FileWriter(new File(SUB_DIR1, "test.properties")), "");

    Properties jobProps1 = new Properties();
    jobProps1.setProperty("k1", "c1");
    jobProps1.setProperty("k3", "b3");
    jobProps1.setProperty("k6", "a6");
    // test-job-conf-dir/test1/test11.pull
    jobProps1.store(new FileWriter(new File(SUB_DIR1, "test11.pull")), "");

    Properties jobProps2 = new Properties();
    jobProps2.setProperty("k7", "a7");
    // test-job-conf-dir/test1/test12.PULL
    jobProps2.store(new FileWriter(new File(SUB_DIR1, "test12.PULL")), "");

    Properties jobProps3 = new Properties();
    jobProps3.setProperty("k1", "d1");
    jobProps3.setProperty("k8", "a8");
    jobProps3.setProperty("k9", "${k8}");
    // test-job-conf-dir/test1/test11/test111.pull
    jobProps3.store(new FileWriter(new File(SUB_DIR11, "test111.pull")), "");

    Properties props2 = new Properties();
    props2.setProperty("k2", "b2");
    props2.setProperty("k5", "a5");
    // test-job-conf-dir/test2/test.properties
    props2.store(new FileWriter(new File(SUB_DIR2, "test.PROPERTIES")), "");

    Properties jobProps4 = new Properties();
    jobProps4.setProperty("k5", "b5");
    // test-job-conf-dir/test2/test21.PULL
    jobProps4.store(new FileWriter(new File(SUB_DIR2, "test21.PULL")), "");
  }

  @Test
  public void testLoadJobConfigs()
      throws ConfigurationException {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONF_ROOT_DIR.getAbsolutePath());
    List<Properties> jobConfigs = SchedulerUtils.loadJobConfigs(properties);
    Assert.assertEquals(jobConfigs.size(), 4);

    // test-job-conf-dir/test1/test11/test111.pull
    Properties jobProps1 = getJobConfigForFile(jobConfigs, "test111.pull");
    Assert.assertEquals(jobProps1.stringPropertyNames().size(), 7);
    Assert.assertTrue(jobProps1.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    Assert.assertTrue(jobProps1.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps1.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps1.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps1.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps1.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps1.getProperty("k9"), "a8");

    // test-job-conf-dir/test1/test11.pull
    Properties jobProps2 = getJobConfigForFile(jobConfigs, "test11.pull");
    Assert.assertEquals(jobProps2.stringPropertyNames().size(), 6);
    Assert.assertTrue(jobProps2.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    Assert.assertTrue(jobProps2.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps2.getProperty("k1"), "c1");
    Assert.assertEquals(jobProps2.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps2.getProperty("k3"), "b3");
    Assert.assertEquals(jobProps2.getProperty("k6"), "a6");

    // test-job-conf-dir/test1/test12.PULL
    Properties jobProps3 = getJobConfigForFile(jobConfigs, "test12.PULL");
    Assert.assertEquals(jobProps3.stringPropertyNames().size(), 6);
    Assert.assertTrue(jobProps3.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    Assert.assertTrue(jobProps3.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps3.getProperty("k1"), "b1");
    Assert.assertEquals(jobProps3.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps3.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps3.getProperty("k7"), "a7");

    // test-job-conf-dir/test2/test21.PULL
    Properties jobProps4 = getJobConfigForFile(jobConfigs, "test21.PULL");
    Assert.assertEquals(jobProps4.stringPropertyNames().size(), 5);
    Assert.assertTrue(jobProps4.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    Assert.assertTrue(jobProps4.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps4.getProperty("k1"), "a1");
    Assert.assertEquals(jobProps4.getProperty("k2"), "b2");
    Assert.assertEquals(jobProps4.getProperty("k5"), "b5");
  }

  @Test(dependsOnMethods = {"testLoadJobConfigs"})
  public void testLoadJobConfigsWithDoneFile()
      throws ConfigurationException, IOException {
    // Create a .done file for test21.pull so it should not be loaded
    Files.copy(new File(SUB_DIR2, "test21.PULL"), new File(SUB_DIR2, "test21.PULL.done"));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONF_ROOT_DIR.getAbsolutePath());
    List<Properties> jobConfigs = SchedulerUtils.loadJobConfigs(properties);
    Assert.assertEquals(jobConfigs.size(), 3);

    // test-job-conf-dir/test1/test11/test111.pull
    Properties jobProps1 = getJobConfigForFile(jobConfigs, "test111.pull");
    Assert.assertEquals(jobProps1.stringPropertyNames().size(), 7);
    Assert.assertEquals(jobProps1.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps1.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps1.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps1.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps1.getProperty("k9"), "a8");

    // test-job-conf-dir/test1/test11.pull
    Properties jobProps2 = getJobConfigForFile(jobConfigs, "test11.pull");
    Assert.assertEquals(jobProps2.stringPropertyNames().size(), 6);
    Assert.assertEquals(jobProps2.getProperty("k1"), "c1");
    Assert.assertEquals(jobProps2.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps2.getProperty("k3"), "b3");
    Assert.assertEquals(jobProps2.getProperty("k6"), "a6");

    // test-job-conf-dir/test1/test12.PULL
    Properties jobProps3 = getJobConfigForFile(jobConfigs, "test12.PULL");
    Assert.assertEquals(jobProps3.stringPropertyNames().size(), 6);
    Assert.assertEquals(jobProps3.getProperty("k1"), "b1");
    Assert.assertEquals(jobProps3.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps3.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps3.getProperty("k7"), "a7");

    Assert.assertNull(getJobConfigForFile(jobConfigs, "test21.PULL"));
  }

  @Test
  public void testLoadJobConfigsForCommonPropsFile()
      throws ConfigurationException, IOException {
    File commonPropsFile = new File(SUB_DIR1, "test.properties");

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONF_ROOT_DIR.getAbsolutePath());
    List<Properties> jobConfigs =
        SchedulerUtils.loadJobConfigs(properties, commonPropsFile, JOB_CONF_ROOT_DIR);
    Assert.assertEquals(jobConfigs.size(), 3);

    // test-job-conf-dir/test1/test11/test111.pull
    Properties jobProps1 = getJobConfigForFile(jobConfigs, "test111.pull");
    Assert.assertEquals(jobProps1.stringPropertyNames().size(), 7);
    Assert.assertEquals(jobProps1.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps1.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps1.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps1.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps1.getProperty("k9"), "a8");

    // test-job-conf-dir/test1/test11.pull
    Properties jobProps2 = getJobConfigForFile(jobConfigs, "test11.pull");
    Assert.assertEquals(jobProps2.stringPropertyNames().size(), 6);
    Assert.assertEquals(jobProps2.getProperty("k1"), "c1");
    Assert.assertEquals(jobProps2.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps2.getProperty("k3"), "b3");
    Assert.assertEquals(jobProps2.getProperty("k6"), "a6");

    // test-job-conf-dir/test1/test12.PULL
    Properties jobProps3 = getJobConfigForFile(jobConfigs, "test12.PULL");
    Assert.assertEquals(jobProps3.stringPropertyNames().size(), 6);
    Assert.assertEquals(jobProps3.getProperty("k1"), "b1");
    Assert.assertEquals(jobProps3.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps3.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps3.getProperty("k7"), "a7");
  }

  @Test
  public void testLoadJobConfig()
      throws ConfigurationException, IOException {
    File jobConfigFile = new File(SUB_DIR11, "test111.pull");
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONF_ROOT_DIR.getAbsolutePath());
    Properties jobProps = SchedulerUtils.loadJobConfig(properties, jobConfigFile, JOB_CONF_ROOT_DIR);

    Assert.assertEquals(jobProps.stringPropertyNames().size(), 7);
    Assert.assertTrue(jobProps.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    Assert.assertTrue(jobProps.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps.getProperty("k2"), "a2");
    Assert.assertEquals(jobProps.getProperty("k3"), "a3");
    Assert.assertEquals(jobProps.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps.getProperty("k9"), "a8");
  }

  @Test(dependsOnMethods = {
      "testLoadJobConfigsWithDoneFile",
      "testLoadJobConfigsForCommonPropsFile",
      "testLoadJobConfig"})
  public void testFileAlterationObserver() throws Exception {
    FileAlterationMonitor monitor = new FileAlterationMonitor(3000);
    final Set<File> fileAltered = Sets.newHashSet();
    final Semaphore semaphore = new Semaphore(0);
    FileAlterationListener listener = new FileAlterationListenerAdaptor() {

      @Override
      public void onFileCreate(File file) {
        fileAltered.add(file);
        semaphore.release();
      }

      @Override
      public void onFileChange(File file) {
        fileAltered.add(file);
        semaphore.release();
      }
    };

    SchedulerUtils.addFileAlterationObserver(monitor, listener, JOB_CONF_ROOT_DIR);

    try {
      monitor.start();
      // Give the monitor some time to start
      Thread.sleep(1000);

      File jobConfigFile = new File(SUB_DIR11, "test111.pull");
      Files.touch(jobConfigFile);

      File commonPropsFile = new File(SUB_DIR1, "test.properties");
      Files.touch(commonPropsFile);

      File newJobConfigFile = new File(SUB_DIR11, "test112.pull");
      Files.append("k1=v1", newJobConfigFile, Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

      semaphore.acquire(3);
      Assert.assertEquals(fileAltered.size(), 3);
      Assert.assertTrue(fileAltered.contains(jobConfigFile));
      Assert.assertTrue(fileAltered.contains(commonPropsFile));
      Assert.assertTrue(fileAltered.contains(newJobConfigFile));
    } finally {
      monitor.stop();
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtil.fullyDelete(JOB_CONF_ROOT_DIR);
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
