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

package org.apache.gobblin.cluster.suite;

import org.junit.Assert;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import static org.apache.gobblin.cluster.SleepingTask.SLEEPING_TASK_SLEEP_TIME;


public class IntegrationJobCancelSuite extends IntegrationBasicSuite {
  public static final String JOB_ID = "job_HelloWorldTestJob_1234";
  public static final String TASK_STATE_FILE = "/tmp/IntegrationJobCancelSuite/taskState/_RUNNING";

  public IntegrationJobCancelSuite(Config jobConfigOverrides) {
    // Put SleepingTask in long sleep allowing cancellation to happen.
    super(jobConfigOverrides.withValue(SLEEPING_TASK_SLEEP_TIME, ConfigValueFactory.fromAnyRef(100)));
  }

  @Override
  public void waitForAndVerifyOutputFiles() throws Exception {
    // SleepingTask is in an infinite sleep. The log line is printed only when a cancellation in invoked.
    Assert.assertTrue(verifyFileForMessage(this.jobLogOutputFile, "Sleep interrupted"));

    // If the job is cancelled, it should not have been able to write 'Hello World!'
    Assert.assertFalse(verifyFileForMessage(this.jobLogOutputFile, "Hello World!"));
  }
}
