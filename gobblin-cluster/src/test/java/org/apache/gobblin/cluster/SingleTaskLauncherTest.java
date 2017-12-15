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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import org.apache.gobblin.util.GobblinProcessBuilder;
import org.apache.gobblin.util.SystemPropertiesWrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SingleTaskLauncherTest {

  private static final String JOB_ID = "1";
  private static final String JAVAHOME = "/javahome";
  private static final String TEST_CLASS_PATH = "foo.jar:bar.jar";
  private static final String WORK_UNIT_PATH = "workUnit.wu";
  private static final String CLUSTER_CONFIG_CONF_PATH = "clusterConfig.conf";

  @Test
  public void testLaunch()
      throws Exception {
    final SystemPropertiesWrapper propertiesWrapper = mock(SystemPropertiesWrapper.class);
    when(propertiesWrapper.getJavaHome()).thenReturn(JAVAHOME);
    when(propertiesWrapper.getJavaClassPath()).thenReturn(TEST_CLASS_PATH);

    final GobblinProcessBuilder processBuilder = mock(GobblinProcessBuilder.class);
    final Process mockProcess = mock(Process.class);
    when(processBuilder.start(any())).thenReturn(mockProcess);

    final Path clusterConfPath = Paths.get(CLUSTER_CONFIG_CONF_PATH);
    final SingleTaskLauncher launcher =
        new SingleTaskLauncher(processBuilder, propertiesWrapper, clusterConfPath);

    final Path workUnitPath = Paths.get(WORK_UNIT_PATH);
    final Process process = launcher.launch(JOB_ID, workUnitPath);

    final List<String> expectedInput = new ArrayList<>(Arrays
        .asList("/javahome/bin/java", "-cp", TEST_CLASS_PATH,
            "org.apache.gobblin.cluster.SingleTaskRunnerMain", "--cluster_config_file_path",
            CLUSTER_CONFIG_CONF_PATH, "--job_id", JOB_ID, "--work_unit_file_path", WORK_UNIT_PATH));
    verify(processBuilder).start(expectedInput);
    assertThat(process).isEqualTo(mockProcess);
  }
}
