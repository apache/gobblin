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

import com.google.common.base.Optional;


/**
 * A factory which can be used to get an instance of {@link TaskScheduler}.
 *
 * @author joelbaranick
 */
public class TaskSchedulerFactory {

  private TaskSchedulerFactory() {}

  /**
   * Gets an instance of the {@link TaskScheduler} with the specified type and ensures that it is started. If
   * the type is unknown an instance of the default {@link TaskScheduler} will be returned.
   *
   * @param type the type of the {@link TaskScheduler}
   * @param name the name associated threads created by the {@link TaskScheduler}
   * @param <K> the type of the key for the {@link ScheduledTask}
   * @param <T> the type of the {@link ScheduledTask}
   * @return an instance of {@link TaskScheduler}
   */
  public static <K, T extends ScheduledTask<K>> TaskScheduler<K, T> get(String type, Optional<String> name) {
    TaskSchedulerType taskSchedulerType = TaskSchedulerType.parse(type);
    return get(taskSchedulerType, name);
  }

  /**
   * Gets an instance of the {@link TaskScheduler} with the specified type and ensures that it is started. If
   * the type is unknown an instance of the default {@link TaskScheduler} will be returned.
   *
   * @param type the type of the {@link TaskScheduler}
   * @param name the name associated threads created by the {@link TaskScheduler}
   * @param <K> the type of the key for the {@link ScheduledTask}
   * @param <T> the type of the {@link ScheduledTask}
   * @return an instance of {@link TaskScheduler}
   */
  public static <K, T extends ScheduledTask<K>> TaskScheduler<K, T> get(TaskSchedulerType type, Optional<String> name) {
    try {
      TaskScheduler<K, T> taskScheduler = type.getTaskSchedulerClass().newInstance();
      taskScheduler.start(name);
      return taskScheduler;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Unable to instantiate task scheduler '" + name + "'.", e);
    }
  }
}
