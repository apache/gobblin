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

import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.junit.Assert;
import org.quartz.JobDataMap;
import org.testng.annotations.Test;


public class FlowTriggerHandlerTest {
  String newCronExpression = "0 0 0 ? * * 2024";
  long newEventToRevisit = 123L;
  long newEventToTrigger = 456L;

  /**
   * Provides an input with all three values (cronExpression, reminderTimestamp, originalEventTime) set in the map
   * Properties and checks that they are updated properly
   */
  @Test
  public void testUpdatePropsInJobDataMap() {
    JobDataMap oldJobDataMap = new JobDataMap();
    Properties originalProperties = new Properties();
    originalProperties.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, "0 0 0 ? * * 2050");
    originalProperties.setProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_REVISIT_TIMESTAMP_MILLIS_KEY, "0");
    originalProperties.setProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_TRIGGER_TIMESTAMP_MILLIS_KEY, "1");
    oldJobDataMap.put(GobblinServiceJobScheduler.PROPERTIES_KEY, originalProperties);

    JobDataMap newJobDataMap = FlowTriggerHandler.updatePropsInJobDataMap(oldJobDataMap, newCronExpression,
        newEventToRevisit, newEventToTrigger);
    Properties newProperties = (Properties) newJobDataMap.get(GobblinServiceJobScheduler.PROPERTIES_KEY);
    Assert.assertEquals(newCronExpression, newProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
    Assert.assertEquals(String.valueOf(newEventToRevisit),
        newProperties.getProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_REVISIT_TIMESTAMP_MILLIS_KEY));
    Assert.assertEquals(String.valueOf(newEventToTrigger),
        newProperties.getProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_TRIGGER_TIMESTAMP_MILLIS_KEY));
  }

  /**
   * Provides input with an empty Properties object and checks that the three values in question are set.
   */
  @Test
  public void testSetPropsInJobDataMap() {
    JobDataMap oldJobDataMap = new JobDataMap();
    Properties originalProperties = new Properties();
    oldJobDataMap.put(GobblinServiceJobScheduler.PROPERTIES_KEY, originalProperties);
    
    JobDataMap newJobDataMap = FlowTriggerHandler.updatePropsInJobDataMap(oldJobDataMap, newCronExpression,
        newEventToRevisit, newEventToTrigger);
    Properties newProperties = (Properties) newJobDataMap.get(GobblinServiceJobScheduler.PROPERTIES_KEY);
    Assert.assertEquals(newCronExpression, newProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
    Assert.assertEquals(String.valueOf(newEventToRevisit),
        newProperties.getProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_REVISIT_TIMESTAMP_MILLIS_KEY));
    Assert.assertEquals(String.valueOf(newEventToTrigger),
        newProperties.getProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_TRIGGER_TIMESTAMP_MILLIS_KEY));
  }
}
