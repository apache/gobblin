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

package org.apache.gobblin.runtime.troubleshooter;

import java.util.Properties;

import org.apache.gobblin.metrics.event.TimingEvent;


public final class TroubleshooterUtils {

  private TroubleshooterUtils() {
  }

  public static String getContextIdForJob(String flowGroup, String flowName, String flowExecutionId, String jobName) {
    return flowGroup + ":" + flowName + ":" + flowExecutionId + ":" + jobName;
  }

  public static String getContextIdForJob(Properties props) {
    return getContextIdForJob(props.getProperty(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD),
                              props.getProperty(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD),
                              props.getProperty(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD),
                              props.getProperty(TimingEvent.FlowEventConstants.JOB_NAME_FIELD));
  }
}
