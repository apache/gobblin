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

package org.apache.gobblin.runtime;

import java.io.StringReader;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.test.TestSource;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * Unit tests for {@link TaskContext}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.runtime"})
public class TaskContextTest {

  private static final String TEST_JOB_CONFIG = "job.name=GobblinTest1\n" +
      "job.group=Test\n" +
      "job.description=Test Gobblin job 1\n" +
      "job.schedule=0 0/1 * * * ?\n" +
      "source.class=org.apache.gobblin.test.TestSource\n" +
      "workunit.namespace=test\n" +
      "workunit.table=test\n" +
      "writer.destination.type=HDFS\n" +
      "writer.output.format=AVRO\n" +
      "writer.fs.uri=file://localhost/\n" +
      "writer.staging.dir=test/staging\n" +
      "writer.output.dir=test/output\n" +
      "writer.file.name=test.avro";

  private TaskContext taskContext;

  @BeforeClass
  public void setUp()
      throws Exception {
    WorkUnit workUnit = WorkUnit.createEmpty();
    Properties properties = new Properties();
    properties.load(new StringReader(TEST_JOB_CONFIG));
    workUnit.addAll(properties);
    workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, JobLauncherUtils.newJobId("GobblinTest1"));
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY,
        JobLauncherUtils.newTaskId(workUnit.getProp(ConfigurationKeys.JOB_ID_KEY), 0));
    this.taskContext = new TaskContext(new WorkUnitState(workUnit));
  }

  @Test
  public void testOtherMethods() {
    Assert.assertTrue(this.taskContext.getSource() instanceof TestSource);
    Assert.assertEquals(this.taskContext.getStatusReportingInterval(),
        ConfigurationKeys.DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS);
    Assert.assertEquals(this.taskContext.getDestinationType(1, 0), Destination.DestinationType.HDFS);
    Assert.assertEquals(this.taskContext.getWriterOutputFormat(1, 0), WriterOutputFormat.AVRO);
    Assert.assertTrue(this.taskContext.getConverters().isEmpty());
  }
}
