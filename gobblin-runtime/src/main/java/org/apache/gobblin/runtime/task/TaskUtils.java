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

package org.apache.gobblin.runtime.task;

import com.google.common.base.Optional;
import org.apache.gobblin.configuration.State;


/**
 * Task utilities.
 */
public class TaskUtils {

  private static final String TASK_FACTORY_CLASS = "org.apache.gobblin.runtime.taskFactoryClass";

  /**
   * Parse the {@link TaskFactory} in the state if one is defined.
   */
  public static Optional<TaskFactory> getTaskFactory(State state) {
    try {
      if (state.contains(TASK_FACTORY_CLASS)) {
        return Optional.of((TaskFactory) Class.forName(state.getProp(TASK_FACTORY_CLASS)).newInstance());
      } else {
        return Optional.absent();
      }
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException("Could not create task factory.", roe);
    }
  }

  /**
   * Define the {@link TaskFactory} that should be used to run this task.
   */
  public static void setTaskFactoryClass(State state, Class<? extends TaskFactory> klazz) {
    state.setProp(TASK_FACTORY_CLASS, klazz.getName());
  }

}
