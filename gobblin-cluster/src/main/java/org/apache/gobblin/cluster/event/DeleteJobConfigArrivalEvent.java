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

package org.apache.gobblin.cluster.event;

import java.util.Properties;

import org.apache.gobblin.annotation.Alpha;


/**
 * A type of events for the deletion of a job configuration to be used with a
 * {@link com.google.common.eventbus.EventBus}.
 *
 */
@Alpha
public class DeleteJobConfigArrivalEvent {

  private final String jobName;
  private final Properties jobConfig;

  public DeleteJobConfigArrivalEvent(String jobName, Properties jobConfig) {
    this.jobName = jobName;
    this.jobConfig = new Properties();
    if (null != jobConfig) {
      this.jobConfig.putAll(jobConfig);
    }
  }

  /**
   * Get the job name.
   *
   * @return the job name
   */
  public String getJobName() {
    return this.jobName;
  }

  /**
   * Get the job config in a {@link Properties} object.
   *
   * @return the job config in a {@link Properties} object
   */
  public Properties getJobConfig() {
    return this.jobConfig;
  }
}
