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
package gobblin.data.management.retention.integration;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.client.ConfigClient;
import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.dataset.CleanableDataset;
import gobblin.data.management.retention.dataset.CleanableDatasetBase;
import gobblin.data.management.retention.profile.ManagedCleanableDatasetFinder;
import gobblin.data.management.retention.profile.MultiCleanableDatasetFinder;
import gobblin.dataset.Dataset;
import gobblin.util.PathUtils;
import gobblin.util.test.RetentionTestDataGenerator;

/**
 *
 * Integration tests for gobblin retention.
 * <ul>
 * <li> Reads a <code>setup_validate.conf</code> to setup files/dirs for the test.
 *  See {@link RetentionTestDataGenerator#setup()}
 * <li> Then runs gobblin retention with confgs in <code>retention.conf</code> file.
 * <li> Validates for deleted and retained files as specified by <code>setup_validate.conf</code> file.
 * See {@link RetentionTestDataGenerator#validate()}
 * </ul>
 *
 * The class is a parameterized test meaning a test {@link #testRetention(String, String)} is run for every row returned
 * by the data provider method {@link #retentionTestDataProvider()}
 *
 * <p>
 * <b>Adding a new test is simple</b>
 * <ul>
 * <li> Create a package under src/test/resources/retentionIntegrationTest/YOUR_NEW_TEST
 * <li> Create a <code>setup_validate.conf</code> file under this package to describe data to create
 * and data to validate after the test
 * <li> Create a <code>retention.conf</code> file under this package with retention configuration. Finders, policies etc.
 * <li> Add YOUR_TEST_NAME to the data provider {@link #retentionTestDataProvider()}
 * </ul>
 * </p>
 */
@Slf4j
@Test(groups = { "SystemTimeTests"})
public class RetentionIntegrationTest {

  private FileSystem fs;
  private Path testClassTempPath;
  private static final String SETUP_VALIDATE_CONFIG_CLASSPATH_FILENAME = "setup_validate.conf";
  private static final String TEST_PACKAGE_RESOURCE_NAME = "retentionIntegrationTest";
  private static final String TEST_DATA_DIR_NAME = "retentionIntegrationTestData";

  @BeforeClass
  public void setupClass() throws Exception {
    this.fs = FileSystem.get(new Configuration());
    testClassTempPath = new Path(RetentionIntegrationTest.class.getClassLoader().getResource("").getFile(), TEST_DATA_DIR_NAME);
    if (!fs.mkdirs(testClassTempPath)) {
      throw new RuntimeException("Failed to create temp directory for the test at " + testClassTempPath.toString());
    }
  }

  /**
   *
   * The method is a data provider for {@link RetentionIntegrationTest#testRetention(String, String)},
   * Return a 2d string array. The pair of strings in each row is passed to {@link RetentionIntegrationTest#testRetention(String, String)}
   * The first element in the pair is the name of the test and 2nd string the name of job config file to use.
   * It may be a .properties file or a .conf file parsed by typesafe
   */
  @DataProvider
  public Object[][] retentionTestDataProvider() {
    return new Object[][] {
        { "testTimeBasedRetention", "retention.conf" },
        { "testTimeBasedRetention", "selection.conf" },
        { "testNewestKRetention", "retention.conf" },
        { "testNewestKRetention", "selection.conf" },
        { "testHourlyPatternRetention", "hourly-retention.job" },
        { "testDailyPatternRetention", "daily-retention.job" },
        { "testMultiVersionRetention", "daily-hourly-retention.conf" },
        { "testCombinePolicy", "retention.job" },
        { "testCombinePolicy", "selection.conf" }
    };
  }

  @Test(dataProvider = "retentionTestDataProvider")
  public void testRetention(final String testName, final String testConfFileName) throws Exception {
    // Temp path for this test under which test data is generated
    Path testNameTempPath = new Path(testClassTempPath, testName);

    RetentionTestDataGenerator dataGenerator = new RetentionTestDataGenerator(testNameTempPath,
        PathUtils.combinePaths(TEST_PACKAGE_RESOURCE_NAME, testName, SETUP_VALIDATE_CONFIG_CLASSPATH_FILENAME), fs);

    dataGenerator.setup();

    clean(fs, PathUtils.combinePaths(TEST_PACKAGE_RESOURCE_NAME, testName, testConfFileName), testNameTempPath);

    dataGenerator.validate();

  }

  /**
   *
   * Does the actual gobblin retention. {@link DatasetCleaner} which does retention in production can no be directly called as we need to resolve some
   * runtime properties like ${testNameTempPath}. This directory contains all the setup data created for a test by {@link RetentionTestDataGenerator#setup()}.
   * It is unique for each test.
   * The default {@link ConfigClient} used by {@link DatasetCleaner} connects to config store configs. We need to provide a
   * mock {@link ConfigClient} since the configs are in classpath and not on config store.
   *
   * @param retentionConfigClasspathResource this is the same jobProps/config files used while running a real retention job
   * @param testNameTempPath temp path for this test where test data is generated
   */
  private static void clean(FileSystem fs, Path retentionConfigClasspathResource, Path testNameTempPath) throws Exception {

    if (retentionConfigClasspathResource.getName().endsWith(".job")) {

      Properties jobProps = new Properties();
      try (final InputStream stream = RetentionIntegrationTest.class.getClassLoader().getResourceAsStream(retentionConfigClasspathResource.toString())) {
        jobProps.load(stream);
        for (Entry<Object, Object> entry : jobProps.entrySet()) {
          jobProps.put(entry.getKey(), StringUtils.replace((String)entry.getValue(), "${testNameTempPath}", testNameTempPath.toString()));
        }
      }
      MultiCleanableDatasetFinder finder = new MultiCleanableDatasetFinder(fs, jobProps);
      for (Dataset dataset : finder.findDatasets()) {
        ((CleanableDataset)dataset).clean();
      }
    } else {
      Config testConfig = ConfigFactory.parseResources(retentionConfigClasspathResource.toString())
          .withFallback(ConfigFactory.parseMap(ImmutableMap.of("testNameTempPath", PathUtils.getPathWithoutSchemeAndAuthority(testNameTempPath).toString()))).resolve();

      ConfigClient client = mock(ConfigClient.class);
      when(client.getConfig(any(String.class))).thenReturn(testConfig);
      Properties jobProps = new Properties();
      jobProps.setProperty(CleanableDatasetBase.SKIP_TRASH_KEY, Boolean.toString(true));
      ManagedCleanableDatasetFinder finder = new ManagedCleanableDatasetFinder(fs, jobProps, testConfig, client);
      for (CleanableDataset dataset : finder.findDatasets()) {
        dataset.clean();
      }
    }
  }

  @AfterClass
  public void cleanUpClass() {
    try {
      this.fs.delete(testClassTempPath, true);
    } catch (Exception e) {
      log.error("Failed to cleanup test files", e);
    }
  }
}
