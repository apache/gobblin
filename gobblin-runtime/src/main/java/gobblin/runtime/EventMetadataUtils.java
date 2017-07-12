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

package gobblin.runtime;

import java.util.List;

import gobblin.configuration.ConfigurationKeys;


/**
 * Utility methods for generating event metadata
 */
public class EventMetadataUtils {
  public static final String TASK_FAILURE_MESSAGE_KEY = "task.failure.message";

  /**
   * Get the number of records written by all the writers
   * @return Sum of the writer records written count across all tasks
   */
  public static long getProcessedCount(List<TaskState> taskStates) {
    long value = 0;

    for (TaskState taskState : taskStates) {
      value += taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN, 0);
    }

    return value;
  }

  /**
   * Get failure messages
   * @return The concatenated failure messages from all the task states
   */
  public static String getTaskFailureExceptions(List<TaskState> taskStates) {
    StringBuffer sb = new StringBuffer();

    // Add task failure messages in a group followed by task failure exceptions
    appendTaskStateValues(taskStates, sb, TASK_FAILURE_MESSAGE_KEY);
    appendTaskStateValues(taskStates, sb, ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY);

    return sb.toString();
  }

  /**
   * Append values for the given key from all {@link TaskState}s
   * @param sb a {@link StringBuffer} to hold the output
   * @param key the key of the values to retrieve
   */
  private static void appendTaskStateValues(List<TaskState> taskStates, StringBuffer sb, String key) {
    // Add task failure messages in a group followed by task failure exceptions
    for (TaskState taskState : taskStates) {
      if (taskState.contains(key)) {
        if (sb.length() != 0) {
          sb.append(",");
        }
        sb.append(taskState.getProp(key));
      }
    }
  }
}
