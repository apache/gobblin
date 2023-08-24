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

package org.apache.gobblin.scheduler;

import com.google.common.base.Optional;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.junit.Assert;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.testng.annotations.Test;


public class JobSchedulerTest {
  // This test creates two triggers with the same job key and job props, but one should have an extra value appended to
  // it.
  @Test
  public void testCreateUniqueTriggersForJob() {
    String jobName = "flow123";
    String jobGroup = "groupA";
    JobKey jobKey = new JobKey(jobName, jobGroup);
    Properties jobProps = new Properties();
    jobProps.put(ConfigurationKeys.JOB_NAME_KEY, jobName);
    jobProps.put(ConfigurationKeys.JOB_GROUP_KEY, jobGroup);
    jobProps.put(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/2 * * * * ?");

    Trigger trigger1 = JobScheduler.createTriggerForJob(jobKey, jobProps, Optional.absent());
    Trigger trigger2 = JobScheduler.createTriggerForJob(jobKey, jobProps, Optional.of("suffix"));

    Assert.assertFalse(trigger1.getKey().equals(trigger2.getKey()));
    Assert.assertTrue(trigger2.getKey().getName().endsWith("suffix"));
  }
}
