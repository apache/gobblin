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

package org.apache.gobblin.azkaban;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;

@Slf4j
public class AzkabanGobblinYarnAppLauncherTest {

  @Test
  public void testOutputConfig() throws IOException {
    File tmpTestDir = Files.createTempDir();

    try {
      Path outputPath = Paths.get(tmpTestDir.toString(), "application.conf");
      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(AzkabanGobblinYarnAppLauncher.AZKABAN_CONFIG_OUTPUT_PATH,
              ConfigValueFactory.fromAnyRef(outputPath.toString()));

      AzkabanGobblinYarnAppLauncher.outputConfigToFile(config);

      String configString = Files.toString(outputPath.toFile(), Charsets.UTF_8);
      Assert.assertTrue(configString.contains("fs"));
    } finally {
      FileUtils.deleteDirectory(tmpTestDir);
    }
  }
}
