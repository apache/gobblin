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

package org.apache.gobblin.util.concurrent;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Enums;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * The types of supported {@link TaskScheduler}s.
 *
 * @author joelbaranick
 */
@AllArgsConstructor
public enum TaskSchedulerType {
  /**
   * A {@link TaskScheduler} based on a {@link java.util.concurrent.ScheduledExecutorService}. This
   * is the default {@link TaskScheduler}.
   */
  SCHEDULEDEXECUTORSERVICE(ScheduledExecutorServiceTaskScheduler.class),

  /**
   * A {@link TaskScheduler} based on a {@link org.jboss.netty.util.HashedWheelTimer}.
   */
  HASHEDWHEELTIMER(HashedWheelTimerTaskScheduler.class);

  @Getter
  private final Class<? extends TaskScheduler> taskSchedulerClass;

  /**
   * Return the {@link TaskSchedulerType} with the specified name. If the specified name
   * does not map to a {@link TaskSchedulerType}, then {@link #SCHEDULEDEXECUTORSERVICE}
   * will be returned.
   *
   * @param name the name of the {@link TaskSchedulerType}
   * @return the specified {@link TaskSchedulerType} or {@link #SCHEDULEDEXECUTORSERVICE}
   */
  public static TaskSchedulerType parse(String name) {
    if (StringUtils.isEmpty(name)) {
      return SCHEDULEDEXECUTORSERVICE;
    }
    return Enums.getIfPresent(TaskSchedulerType.class, name.toUpperCase()).or(SCHEDULEDEXECUTORSERVICE);
  }
}
