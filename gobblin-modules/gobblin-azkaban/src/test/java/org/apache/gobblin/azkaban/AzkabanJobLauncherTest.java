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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.JobLauncherFactory;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class AzkabanJobLauncherTest {

  @Test
  public void testDisableTokenInitialization() throws Exception {
    Properties props = new Properties();

    props.setProperty(ConfigurationKeys.JOB_NAME_KEY, "job1");
    props.setProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherFactory.JobLauncherType.LOCAL.name());
    props.setProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, "false");
    props.setProperty(ConfigurationKeys.STATE_STORE_ENABLED, "false");
    props.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, DummySource.class.getName());

    // Should get an error since tokens are initialized by default
    try {
      new AzkabanJobLauncher("test", props);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Missing required property keytab.user"));
    }

    // No error expected since initialization is skipped
    props.setProperty(AzkabanJobLauncher.GOBBLIN_AZKABAN_INITIALIZE_HADOOP_TOKENS, "false");
    new AzkabanJobLauncher("test", props);
  }

  /**
   * A dummy implementation of {@link Source}.
   */
  public static class DummySource extends AbstractSource<String, Integer> {
    @Override
    public List<WorkUnit> getWorkunits(SourceState sourceState) {
      return Collections.EMPTY_LIST;
    }

    @Override
    public Extractor<String, Integer> getExtractor(WorkUnitState state) throws IOException {
      return null;
    }

    @Override
    public void shutdown(SourceState state) {
    }
  }
}
