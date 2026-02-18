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
package org.apache.gobblin.yarn;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


public class YarnHelixUtilsTest {
  /**
   * Uses the token file created using {@link GobblinYarnTestUtils#createTokenFileForService(Path, String)} method and
   * added to the resources folder.
   * @throws IOException
   */
  String tempDir = Files.createTempDir().getPath();

  @AfterClass
  public void tearDown() throws IOException{
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(new Path(this.tempDir), true);
  }

  @Test
  public void testUpdateToken()
      throws IOException {
    //Ensure the credentials is empty on start
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Assert.assertNull(credentials.getToken(new Text("testService")));

    //Attempt reading a non-existent token file and ensure credentials object has no tokens
    YarnHelixUtils.updateToken(".token1");
    credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Assert.assertNull(credentials.getToken(new Text("testService")));

    //Read a valid token file and ensure the credentials object has a valid token
    YarnHelixUtils.updateToken(GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Token<?> readToken = credentials.getToken(new Text("testService"));
    Assert.assertNotNull(readToken);
  }

  @Test
  public void testGetJarCachePath() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    // 1726074000013L = Sept 11, 2024 15:40:00 GMT (day 11, first half of month)
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726074000013L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef("/tmp"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, mockFs);

    // Sept 11 is in first half (1-15), so expect yyyy-MM.1 format
    Assert.assertEquals(jarCachePath, new Path("/tmp/2024-09.1"));
  }

  @Test
  public void testGetJarCachePath_SecondHalfOfMonth() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    // 1726596000000L = Sept 17, 2024 16:00:00 GMT (day 17, second half of month)
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726596000000L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef("/tmp"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, mockFs);

    // Sept 17 is in second half (16-end), so expect yyyy-MM.2 format
    Assert.assertEquals(jarCachePath, new Path("/tmp/2024-09.2"));
  }

  @Test
  public void testGetJarCachePath_BoundaryDay15() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    // 1726358400000L = Sept 15, 2024 00:00:00 GMT (day 15, still first half)
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726358400000L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef("/tmp"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, mockFs);

    // Sept 15 is the last day of first half
    Assert.assertEquals(jarCachePath, new Path("/tmp/2024-09.1"));
  }

  @Test
  public void testGetJarCachePath_BoundaryDay16() throws IOException {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    // 1726444800000L = Sept 16, 2024 00:00:00 GMT (day 16, second half)
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726444800000L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef("/tmp"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, mockFs);

    // Sept 16 is the first day of second half
    Assert.assertEquals(jarCachePath, new Path("/tmp/2024-09.2"));
  }

  @Test
  public void retainLatestKJarCachePaths() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726074000013L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef(this.tempDir + "/tmp"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, fs);
    fs.mkdirs(jarCachePath);
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08.2"));
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08.1"));
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-07.2"));
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-07.1"));

    // Retain 3 latest periods (current + 2 previous)
    YarnHelixUtils.retainKLatestJarCachePaths(jarCachePath.getParent(), 3, fs);

    // Should keep the 3 latest
    Assert.assertTrue(fs.exists(new Path(this.tempDir, "tmp/2024-09.1")));
    Assert.assertTrue(fs.exists(new Path(this.tempDir, "tmp/2024-08.2")));
    Assert.assertTrue(fs.exists(new Path(this.tempDir, "tmp/2024-08.1")));
    // Should be cleaned up
    Assert.assertFalse(fs.exists(new Path(this.tempDir, "tmp/2024-07.2")));
    Assert.assertFalse(fs.exists(new Path(this.tempDir, "tmp/2024-07.1")));
  }

  @Test
  public void retainLatestKJarCachePaths_OnlyOldMonthlyFormat() throws IOException {
    // Test cleanup with only old monthly format directories (pre-migration scenario)
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path testDir = new Path(this.tempDir, "tmp-old-only");

    // Create only old monthly format directories
    fs.mkdirs(new Path(testDir, "2024-09"));  // Current (most recent)
    fs.mkdirs(new Path(testDir, "2024-08"));
    fs.mkdirs(new Path(testDir, "2024-07"));
    fs.mkdirs(new Path(testDir, "2024-06"));
    fs.mkdirs(new Path(testDir, "2024-05"));

    // Retain 3 latest periods
    YarnHelixUtils.retainKLatestJarCachePaths(testDir, 3, fs);

    // Should keep the 3 most recent monthly directories
    Assert.assertTrue("2024-09 should be kept", fs.exists(new Path(testDir, "2024-09")));
    Assert.assertTrue("2024-08 should be kept", fs.exists(new Path(testDir, "2024-08")));
    Assert.assertTrue("2024-07 should be kept", fs.exists(new Path(testDir, "2024-07")));

    // These should be deleted
    Assert.assertFalse("2024-06 should be deleted", fs.exists(new Path(testDir, "2024-06")));
    Assert.assertFalse("2024-05 should be deleted", fs.exists(new Path(testDir, "2024-05")));
  }

  @Test
  public void retainLatestKJarCachePaths_MixedFormats() throws IOException {
    // Test cleanup with both old monthly and new semi-monthly formats
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726596000000L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef(this.tempDir + "/tmp-mixed"))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, ConfigValueFactory.fromAnyRef(true));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config, fs);
    // Current path is 2024-09.2
    fs.mkdirs(jarCachePath);

    // Create mix of old monthly and new semi-monthly formats
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-09.1"));  // New format
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-09"));    // Old format (treated as 2024-09.1)
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08.2"));  // New format
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08"));    // Old format (treated as 2024-08.1)
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08.1"));  // New format
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-07"));    // Old format (should be deleted)

    // Retain 3 latest periods
    YarnHelixUtils.retainKLatestJarCachePaths(jarCachePath.getParent(), 3, fs);

    // Expected retention (keeping 3 latest):
    // After normalization and sorting, the order is:
    // 1. 2024-07 (normalized to 2024-07.1)
    // 2. 2024-08 (normalized to 2024-08.1)
    // 3. 2024-08.1 (normalized to 2024-08.1)
    // 4. 2024-08.2 (normalized to 2024-08.2)
    // 5. 2024-09 (normalized to 2024-09.1)
    // 6. 2024-09.1 (normalized to 2024-09.1)
    // 7. 2024-09.2 (normalized to 2024-09.2)
    //
    // Keeping k=3 latest means we keep entries 5, 6, 7:
    // Keep: 2024-09 (old), 2024-09.1, 2024-09.2
    // Delete: Everything from August and July

    Assert.assertTrue("Current period 2024-09.2 should be kept", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-09.2")));
    Assert.assertTrue("2024-09.1 should be kept", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-09.1")));
    Assert.assertTrue("Old format 2024-09 should be kept (sorts same as 2024-09.1)", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-09")));

    // All August and July directories should be cleaned up
    Assert.assertFalse("2024-08.2 should be deleted", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-08.2")));
    Assert.assertFalse("Old format 2024-08 should be deleted", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-08")));
    Assert.assertFalse("2024-08.1 should be deleted", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-08.1")));
    Assert.assertFalse("Old format 2024-07 should be deleted", fs.exists(new Path(this.tempDir, "tmp-mixed/2024-07")));
  }

  @Test
  public void retainLatestKJarCachePaths_EarlyMigrationPhase() throws IOException {
    // Test early migration: mostly old monthly with just one new semi-monthly
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path testDir = new Path(this.tempDir, "tmp-early-migration");

    // Create mostly old format with one new semi-monthly (simulating first deployment)
    fs.mkdirs(new Path(testDir, "2024-10.1"));  // NEW: First semi-monthly period created
    fs.mkdirs(new Path(testDir, "2024-09"));    // OLD: Previous monthly
    fs.mkdirs(new Path(testDir, "2024-08"));    // OLD
    fs.mkdirs(new Path(testDir, "2024-07"));    // OLD
    fs.mkdirs(new Path(testDir, "2024-06"));    // OLD

    // Retain 3 latest periods
    YarnHelixUtils.retainKLatestJarCachePaths(testDir, 3, fs);

    // After sorting with normalization (old format treated as .1):
    // 2024-06 -> 2024-06.1
    // 2024-07 -> 2024-07.1
    // 2024-08 -> 2024-08.1
    // 2024-09 -> 2024-09.1
    // 2024-10.1 -> 2024-10.1 (stays)
    //
    // Keep last 3: 2024-08, 2024-09, 2024-10.1

    Assert.assertTrue("New format 2024-10.1 should be kept", fs.exists(new Path(testDir, "2024-10.1")));
    Assert.assertTrue("Old format 2024-09 should be kept", fs.exists(new Path(testDir, "2024-09")));
    Assert.assertTrue("Old format 2024-08 should be kept", fs.exists(new Path(testDir, "2024-08")));

    // These should be deleted
    Assert.assertFalse("Old format 2024-07 should be deleted", fs.exists(new Path(testDir, "2024-07")));
    Assert.assertFalse("Old format 2024-06 should be deleted", fs.exists(new Path(testDir, "2024-06")));
  }

  @Test
  public void retainLatestKJarCachePaths_EmptyDirectory() throws IOException {
    // Test edge case: empty parent directory
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path testDir = new Path(this.tempDir, "tmp-empty");
    fs.mkdirs(testDir);

    // Should not throw exception on empty directory
    boolean result = YarnHelixUtils.retainKLatestJarCachePaths(testDir, 3, fs);
    Assert.assertTrue("Should return true for empty directory", result);
  }

  @Test
  public void testGetJarListFromConfigs() {
    // Test when container jars is empty
    Config emptyContainerJarsList = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LIB_JAR_LIST, ConfigValueFactory.fromAnyRef("a.jar,b.jar"))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY, ConfigValueFactory.fromAnyRef(""));

    Set<String> jars = YarnHelixUtils.getAppLibJarList(emptyContainerJarsList);
    Assert.assertEquals(2, jars.size());
    Assert.assertTrue(jars.contains("a.jar"));
    Assert.assertTrue(jars.contains("b.jar"));

    // Test when yarn application lib jars is empty
    Config emptyYarnAppLibJarsConfig = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LIB_JAR_LIST, ConfigValueFactory.fromAnyRef(""))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY, ConfigValueFactory.fromAnyRef("c.jar,d.jar"));

    jars = YarnHelixUtils.getAppLibJarList(emptyYarnAppLibJarsConfig);
    Assert.assertEquals(2, jars.size());
    Assert.assertTrue(jars.contains("c.jar"));
    Assert.assertTrue(jars.contains("d.jar"));

    // Test when both yarn application lib jars and container jars are not empty
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LIB_JAR_LIST, ConfigValueFactory.fromAnyRef("a.jar,b.jar"))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY, ConfigValueFactory.fromAnyRef("c.jar,d.jar"));
    jars = YarnHelixUtils.getAppLibJarList(config);
    Assert.assertEquals(4, jars.size());
    Assert.assertTrue(jars.contains("a.jar"));
    Assert.assertTrue(jars.contains("b.jar"));
    Assert.assertTrue(jars.contains("c.jar"));
    Assert.assertTrue(jars.contains("d.jar"));
  }
}