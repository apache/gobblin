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
package org.apache.gobblin.service.modules.orchestration;

import java.net.URI;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.util.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


@Slf4j
@Test(groups = { "org.apache.gobblin.service.modules.orchestration" })
public class AzkabanProjectConfigTest {

  @Test
  public void testProjectNameDefault() throws Exception {
    String expectedProjectName = "GobblinService__uri";

    Properties properties = new Properties();
    JobSpec jobSpec = new JobSpec(new URI("uri"), "0.0", "test job spec",
        ConfigUtils.propertiesToConfig(properties), properties, Optional.absent());
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig(jobSpec);

    String actualProjectName = azkabanProjectConfig.getAzkabanProjectName();

    Assert.assertEquals(actualProjectName, expectedProjectName);
  }

  @Test
  public void testProjectNameWithConfig() throws Exception {
    String expectedProjectName = "randomPrefix_http___localhost_8000_context";

    Properties properties = new Properties();
    properties.setProperty("gobblin.service.azkaban.project.namePrefix", "randomPrefix");
    JobSpec jobSpec = new JobSpec(new URI("http://localhost:8000/context"), "0.0", "test job spec",
        ConfigUtils.propertiesToConfig(properties), properties, Optional.absent());
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig(jobSpec);

    String actualProjectName = azkabanProjectConfig.getAzkabanProjectName();

    Assert.assertEquals(actualProjectName, expectedProjectName);
  }

  @Test
  public void testProjectNameWithReallyLongName() throws Exception {
    String expectedProjectName = "randomPrefixWithReallyLongName_http___localhost_8000__55490420";

    Properties properties = new Properties();
    properties.setProperty("gobblin.service.azkaban.project.namePrefix", "randomPrefixWithReallyLongName");
    JobSpec jobSpec = new JobSpec(new URI("http://localhost:8000/context/that-keeps-expanding-and-explanding"),
        "0.0", "test job spec", ConfigUtils.propertiesToConfig(properties), properties, Optional.absent());
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig(jobSpec);

    String actualProjectName = azkabanProjectConfig.getAzkabanProjectName();

    Assert.assertEquals(actualProjectName, expectedProjectName);
  }

  @Test
  public void testProjectZipFileName() throws Exception {
    String expectedZipFileName = "randomPrefix_http___localhost_8000_context.zip";

    Properties properties = new Properties();
    properties.setProperty("gobblin.service.azkaban.project.namePrefix", "randomPrefix");
    JobSpec jobSpec = new JobSpec(new URI("http://localhost:8000/context"), "0.0", "test job spec",
        ConfigUtils.propertiesToConfig(properties), properties, Optional.absent());
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig(jobSpec);

    String actualZipFileName = azkabanProjectConfig.getAzkabanProjectZipFilename();

    Assert.assertEquals(actualZipFileName, expectedZipFileName);
  }

  @Test
  public void testProjectZipFileNameForLongName() throws Exception {
    String expectedZipFileName = "randomPrefixWithReallyLongName_http___localhost_8000__55490420.zip";

    Properties properties = new Properties();
    properties.setProperty("gobblin.service.azkaban.project.namePrefix", "randomPrefixWithReallyLongName");
    JobSpec jobSpec = new JobSpec(new URI("http://localhost:8000/context/that-keeps-expanding-and-explanding"),
        "0.0", "test job spec", ConfigUtils.propertiesToConfig(properties), properties, Optional.absent());
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig(jobSpec);

    String actualZipFileName = azkabanProjectConfig.getAzkabanProjectZipFilename();

    Assert.assertEquals(actualZipFileName, expectedZipFileName);
  }
}
