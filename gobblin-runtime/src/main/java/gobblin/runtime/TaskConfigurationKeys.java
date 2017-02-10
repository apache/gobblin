/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.runtime;

/**
 * A class that contains configuration keys for a {@link Task}
 */
public class TaskConfigurationKeys {
  /**
   * Configuration properties related to continuous / streaming mode
   */
  public static final String TASK_EXECUTION_MODE = "task.executionMode";
  public static final String DEFAULT_TASK_EXECUTION_MODE = "BATCH";
  public static final String STREAMING_WATERMARK_COMMIT_INTERVAL_MILLIS = "streaming.watermark.commitIntervalMillis";
  public static final Long DEFAULT_STREAMING_WATERMARK_COMMIT_INTERVAL_MILLIS = 2000L; // 2 seconds per commit

  /**
   * Configuration properties related to optimizations for single branch tasks
   */
  public static final String TASK_IS_SINGLE_BRANCH_SYNCHRONOUS = "gobblin.task.is.single.branch.synchronous";
  public static final String DEFAULT_TASK_IS_SINGLE_BRANCH_SYNCHRONOUS = Boolean.toString(false);

}
