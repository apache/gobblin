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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
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
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726074000013L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef("/tmp"));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config);

    Assert.assertEquals(jarCachePath, new Path("/tmp/2024-09"));
  }

  @Test
  public void retainLatestKJarCachePaths() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Config config = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY, ConfigValueFactory.fromAnyRef(1726074000013L))
        .withValue(GobblinYarnConfigurationKeys.JAR_CACHE_DIR, ConfigValueFactory.fromAnyRef(this.tempDir + "/tmp"));
    Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(config);
    fs.mkdirs(jarCachePath);
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-08"));
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-07"));
    fs.mkdirs(new Path(jarCachePath.getParent(), "2024-06"));

    YarnHelixUtils.retainKLatestJarCachePaths(jarCachePath.getParent(), 2, fs);

    Assert.assertTrue(fs.exists(new Path(this.tempDir, "tmp/2024-09")));
    Assert.assertTrue(fs.exists(new Path(this.tempDir, "tmp/2024-08")));
    // Should be cleaned up
    Assert.assertFalse(fs.exists(new Path(this.tempDir, "tmp/2024-07")));
    Assert.assertFalse(fs.exists(new Path(this.tempDir, "tmp/2024-06")));

  }
}