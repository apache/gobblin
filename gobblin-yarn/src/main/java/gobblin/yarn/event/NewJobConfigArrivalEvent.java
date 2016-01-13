/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn.event;

import java.util.Properties;


/**
 * A type of events for the arrival of a new job configuration to be used with a
 * {@link com.google.common.eventbus.EventBus}.
 *
 * @author Yinan Li
 */
public class NewJobConfigArrivalEvent {

  private final String jobName;
  private final Properties jobConfig;

  public NewJobConfigArrivalEvent(String jobName, Properties jobConfig) {
    this.jobName = jobName;
    this.jobConfig = new Properties();
    this.jobConfig.putAll(jobConfig);
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
