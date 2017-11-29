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

package org.apache.gobblin.cluster;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

public class GobblinClusterUtilsTest {

  FileSystem fs = mock(FileSystem.class);

  @Test
  public void work_dir_should_get_value_from_config_when_specified() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("gobblin.cluster.workDir", "/foo/bar");

    Config config = ConfigFactory.parseMap(configMap);

    Path workDirPath = GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, fs, "appName", "appid");

    assertEquals(new Path("/foo/bar"), workDirPath);

  }

  @Test
  public void work_dir_should_get_default_calculated_value_when_not_specified() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    Config config = ConfigFactory.parseMap(configMap);

    when(fs.getHomeDirectory()).thenReturn(new Path("/home/"));

    Path workDirPath = GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, fs, "appName", "appid");

    assertEquals(new Path("/home/appName/appid"), workDirPath);
  }
}
