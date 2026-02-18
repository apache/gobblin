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

package org.apache.gobblin.temporal.yarn;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/**
 * Unit tests for {@link GobblinTemporalApplicationMaster}, in particular the config-based
 * work directory cleanup used in the shutdown hook.
 */
@Test(groups = { "gobblin.temporal" })
public class GobblinTemporalApplicationMasterTest {

  /**
   * Verifies that {@link GobblinTemporalApplicationMaster#cleanupWorkDirsFromConfig(Config)}
   * deletes work directories when cleanup is enabled.
   */
  @Test
  public void testCleanupWorkDirsFromConfigDeletesDirs() throws Exception {
    Path baseDir = Files.createTempDirectory("GobblinTemporalAMTest");
    try {
      Path stagingDir = baseDir.resolve("staging");
      Path outputDir = baseDir.resolve("output");
      Files.createDirectories(stagingDir);
      Files.createDirectories(outputDir);
      Assert.assertTrue(Files.exists(stagingDir));
      Assert.assertTrue(Files.exists(outputDir));

      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(ConfigurationKeys.WRITER_STAGING_DIR, ConfigValueFactory.fromAnyRef(stagingDir.toAbsolutePath().toString()))
          .withValue(ConfigurationKeys.WRITER_OUTPUT_DIR, ConfigValueFactory.fromAnyRef(outputDir.toAbsolutePath().toString()))
          .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, ConfigValueFactory.fromAnyRef("true"));

      GobblinTemporalApplicationMaster.cleanupWorkDirsFromConfig(config);

      Assert.assertFalse(Files.exists(stagingDir), "Staging dir should be deleted");
      Assert.assertFalse(Files.exists(outputDir), "Output dir should be deleted");
    } finally {
      FileUtils.deleteDirectory(baseDir.toFile());
    }
  }

  /**
   * Verifies that cleanup is a no-op when neither paths nor work dir is set.
   */
  @Test
  public void testCleanupWorkDirsFromConfigNoPaths() throws Exception {
    Path baseDir = Files.createTempDirectory("GobblinTemporalAMTest");
    try {
      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, ConfigValueFactory.fromAnyRef("true"));
      // No WRITER_STAGING_DIR or WRITER_OUTPUT_DIR or job info

      GobblinTemporalApplicationMaster.cleanupWorkDirsFromConfig(config);

      // Base dir should still exist (we didn't pass it in config)
      Assert.assertTrue(Files.exists(baseDir));
    } finally {
      FileUtils.deleteDirectory(baseDir.toFile());
    }
  }

  /**
   * Verifies that cleanup only deletes the paths that exist; non-existent paths are skipped.
   */
  @Test
  public void testCleanupWorkDirsFromConfigSkipsNonExistent() throws Exception {
    Path baseDir = Files.createTempDirectory("GobblinTemporalAMTest");
    try {
      Path stagingDir = baseDir.resolve("staging");
      Files.createDirectories(stagingDir);
      Path outputDir = baseDir.resolve("output");
      // Do not create outputDir

      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(ConfigurationKeys.WRITER_STAGING_DIR, ConfigValueFactory.fromAnyRef(stagingDir.toAbsolutePath().toString()))
          .withValue(ConfigurationKeys.WRITER_OUTPUT_DIR, ConfigValueFactory.fromAnyRef(outputDir.toAbsolutePath().toString()))
          .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, ConfigValueFactory.fromAnyRef("true"));

      GobblinTemporalApplicationMaster.cleanupWorkDirsFromConfig(config);

      Assert.assertFalse(Files.exists(stagingDir), "Staging dir should be deleted");
      Assert.assertFalse(Files.exists(outputDir), "Output dir should still not exist");
    } finally {
      FileUtils.deleteDirectory(baseDir.toFile());
    }
  }

  /**
   * Verifies that cleanup is disabled when GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED is false.
   */
  @Test
  public void testCleanupWorkDirsFromConfigDisabled() throws Exception {
    Path baseDir = Files.createTempDirectory("GobblinTemporalAMTest");
    try {
      Path stagingDir = baseDir.resolve("staging");
      Files.createDirectories(stagingDir);

      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(ConfigurationKeys.WRITER_STAGING_DIR, ConfigValueFactory.fromAnyRef(stagingDir.toAbsolutePath().toString()))
          .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, ConfigValueFactory.fromAnyRef("false"));

      GobblinTemporalApplicationMaster.cleanupWorkDirsFromConfig(config);

      Assert.assertTrue(Files.exists(stagingDir), "Staging dir should still exist when cleanup is disabled");
    } finally {
      FileUtils.deleteDirectory(baseDir.toFile());
    }
  }
}
