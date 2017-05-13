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

package gobblin.runtime.job_catalog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.runtime.job_catalog.ImmutableFSJobCatalog;
import gobblin.util.ConfigUtils;
import gobblin.util.PullFileLoader;
import gobblin.util.filesystem.PathAlterationObserverScheduler;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;
import gobblin.util.filesystem.PathAlterationObserver;


/**
 * Inherit original Testing for loading configurations, with .properties files.
 * The testing folder structure is:
 * /root
 *  - root.properties
 *  /test1
 *    - test11.pull
 *    - test12.pull
 *    /test11
 *      - test111.pull
 *  /test2
 *    - test.properties
 *    - test21.pull
 * The new testing routine for JobSpec,
 * is to create a jobSpec( Simulated as the result of external JobSpecMonitor)
 * persist it, reload it from file system, and compare with the original JobSpec.
 *
 */

@Test(groups = {"gobblin.runtime"})
public class FSJobCatalogHelperTest {

  // For general type of File system
  private File jobConfigDir;
  private File subDir1;
  private File subDir11;
  private File subDir2;

  private Config sysConfig;
  private PullFileLoader loader;
  private ImmutableFSJobCatalog.JobSpecConverter converter;

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


    this.sysConfig = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, this.jobConfigDir.getAbsolutePath())
        .build());
    ImmutableFSJobCatalog.ConfigAccessor cfgAccess =
        new ImmutableFSJobCatalog.ConfigAccessor(this.sysConfig);
    this.loader = new PullFileLoader(new Path(jobConfigDir.toURI()), FileSystem.get(new Configuration()),
        cfgAccess.getJobConfigurationFileExtensions(),
        PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
    this.converter = new ImmutableFSJobCatalog.JobSpecConverter(new Path(this.jobConfigDir.toURI()), Optional.of(
        FSJobCatalog.CONF_EXTENSION));

    Properties rootProps = new Properties();
    rootProps.setProperty("k1", "a1");
    rootProps.setProperty("k2", "a2");
    // test-job-conf-dir/root.properties
    rootProps.store(new FileWriter(new File(this.jobConfigDir, "root.properties")), "");

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

  // This test doesn't delete framework attributes and
  @Test
  public void testloadGenericJobConfigs()
      throws ConfigurationException, IOException, URISyntaxException {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, this.jobConfigDir.getAbsolutePath());
    List<JobSpec> jobSpecs = Lists.transform(
        Lists.newArrayList(loader.loadPullFilesRecursively(loader.getRootDirectory(), this.sysConfig, false)),
        this.converter);

    List<Properties> jobConfigs = convertJobSpecList2PropList(jobSpecs);
    Assert.assertEquals(jobConfigs.size(), 4);

    // test-job-conf-dir/test1/test11/test111.pull
    Properties jobProps1 = getJobConfigForFile(jobConfigs, "test111.pull");

    //5 is consisting of three attributes, plus ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY
    // which is on purpose to keep
    // plus ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, which is not necessary to convert into JobSpec
    // but keep it here to avoid NullPointer exception and validation purpose for testing.
    Assert.assertEquals(jobProps1.stringPropertyNames().size(), 5);
    Assert.assertTrue(jobProps1.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps1.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY),
        this.jobConfigDir.getAbsolutePath());
    Assert.assertEquals(jobProps1.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps1.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps1.getProperty("k9"), "a8");

    // test-job-conf-dir/test1/test11.pull
    Properties jobProps2 = getJobConfigForFile(jobConfigs, "test11.pull");
    Assert.assertEquals(jobProps2.stringPropertyNames().size(), 5);
    Assert.assertTrue(jobProps2.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps2.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY),
                        this.jobConfigDir.getAbsolutePath());
    Assert.assertEquals(jobProps2.getProperty("k1"), "c1");
    Assert.assertEquals(jobProps2.getProperty("k3"), "b3");
    Assert.assertEquals(jobProps2.getProperty("k6"), "a6");

    // test-job-conf-dir/test1/test12.PULL
    Properties jobProps3 = getJobConfigForFile(jobConfigs, "test12.PULL");
    Assert.assertEquals(jobProps3.stringPropertyNames().size(), 3);
    Assert.assertTrue(jobProps3.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps3.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY),
        this.jobConfigDir.getAbsolutePath());
    Assert.assertEquals(jobProps3.getProperty("k7"), "a7");

    // test-job-conf-dir/test2/test21.PULL
    Properties jobProps4 = getJobConfigForFile(jobConfigs, "test21.PULL");
    Assert.assertEquals(jobProps4.stringPropertyNames().size(), 3);
    Assert.assertTrue(jobProps4.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
    Assert.assertEquals(jobProps4.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY),
        this.jobConfigDir.getAbsolutePath());
    Assert.assertEquals(jobProps4.getProperty("k5"), "b5");
  }

  @Test(dependsOnMethods = {"testloadGenericJobConfigs"})
  public void testloadGenericJobConfig()
      throws ConfigurationException, IOException {
    Path jobConfigPath = new Path(this.subDir11.getAbsolutePath(), "test111.pull");

    Properties jobProps =
        ConfigUtils.configToProperties(loader.loadPullFile(jobConfigPath, this.sysConfig, false));

    Assert.assertEquals(jobProps.stringPropertyNames().size(), 5);
    Assert.assertEquals(jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY),
        this.jobConfigDir.getAbsolutePath());
    Assert.assertEquals(jobProps.getProperty("k1"), "d1");
    Assert.assertEquals(jobProps.getProperty("k8"), "a8");
    Assert.assertEquals(jobProps.getProperty("k9"), "a8");
  }

  @Test(dependsOnMethods = {"testloadGenericJobConfig"})
  public void testPathAlterationObserver()
      throws Exception {
    PathAlterationObserverScheduler detector = new PathAlterationObserverScheduler(1000);
    final Set<Path> fileAltered = Sets.newHashSet();
    final Semaphore semaphore = new Semaphore(0);
    PathAlterationListener listener = new PathAlterationListenerAdaptor() {

      @Override
      public void onFileCreate(Path path) {
        fileAltered.add(path);
        semaphore.release();
      }

      @Override
      public void onFileChange(Path path) {
        fileAltered.add(path);
        semaphore.release();
      }
    };

    detector.addPathAlterationObserver(listener, Optional.<PathAlterationObserver>absent(),
        new Path(this.jobConfigDir.getPath()));
    try {
      detector.start();
      // Give the monitor some time to start
      Thread.sleep(1000);

      File jobConfigFile = new File(this.subDir11, "test111.pull");
      Files.touch(jobConfigFile);

      File newJobConfigFile = new File(this.subDir11, "test112.pull");
      Files.append("k1=v1", newJobConfigFile, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);

      semaphore.acquire(2);
      Assert.assertEquals(fileAltered.size(), 2);

      Assert.assertTrue(fileAltered.contains(new Path("file:" + jobConfigFile)));
      Assert.assertTrue(fileAltered.contains(new Path("file:" + newJobConfigFile)));
    } finally {
      detector.stop();
    }
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

  /**
   * Suppose in the testing routine, each JobSpec will at least have either config or properties.
   * @param jobConfigs
   * @return
   */
  private List<Properties> convertJobSpecList2PropList(List<JobSpec> jobConfigs) {
    List<Properties> result = Lists.newArrayList();
    for (JobSpec js : jobConfigs) {
      Properties propToBeAdded;
      if (js.getConfigAsProperties() != null) {
        propToBeAdded = js.getConfigAsProperties();
      } else {
        propToBeAdded = ConfigUtils.configToProperties(js.getConfig());
      }

      // For the testing purpose, added it back when doing the comparison.
      propToBeAdded.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, js.getUri().toString());
      result.add(propToBeAdded);
    }
    return result;
  }
}
